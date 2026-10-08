"""相对于根目录的文件清单及独立的不可变快照。"""
import json
import logging
import os
import shutil
import uuid
from pathlib import Path, PurePosixPath

from .config import safe_key
from .manifest import digest_file

log = logging.getLogger(__name__)


def matches(path, patterns):
    p = PurePosixPath(path)
    return any(p.match(pattern) or (pattern.startswith("**/") and p.match(pattern[3:])) for pattern in patterns)


def atomic_bytes(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("wb") as out:
        out.write(data)
        out.flush()
        os.fsync(out.fileno())
    os.replace(temporary, path)


class FileCollector:
    def __init__(self, config, state, stop=None):
        self.config, self.state, self.stop = config, state, stop

    def _check_stop(self):
        if self.stop and self.stop.is_set():
            raise InterruptedError("stopping")

    def scan(self, source, now):
        if not source.root.is_dir():
            raise OSError("source directory unavailable")
        initial = not self.state.one("SELECT initialized FROM sources WHERE id=?", (source.id,))[0]
        seen, errors = set(), []
        for directory, dirs, names in os.walk(source.root, onerror=errors.append, followlinks=False):
            dirs[:] = sorted(d for d in dirs if not (Path(directory) / d).is_symlink()) if source.recursive else []
            for name in sorted(names):
                self._check_stop()
                path = Path(directory) / name
                relative = path.relative_to(source.root).as_posix()
                if not matches(relative, source.include) or matches(relative, source.exclude):
                    continue
                try:
                    safe_key(relative)
                    if path.is_symlink() or not path.is_file():
                        continue
                    seen.add(relative)
                    self._collect(source, path, relative, now, initial)
                except InterruptedError:
                    raise
                except (OSError, ValueError) as error:
                    code = type(error).__name__
                    self.state.db.execute("UPDATE files SET error=? WHERE source=? AND path=?", (code, source.id, relative))
                    log.error("file_capture_failed", extra={"source": source.id, "code": code})
        if errors:
            raise errors[0]
        self.state.db.execute("UPDATE sources SET initialized=1 WHERE id=?", (source.id,))
        for row in self.state.all("SELECT path FROM files WHERE source=?", (source.id,)):
            if row["path"] not in seen:
                self.state.db.execute("UPDATE files SET status='MISSING',error='SOURCE_MISSING',changed=? WHERE source=? AND path=?",
                                      (now, source.id, row["path"]))

    def _collect(self, source, path, relative, now, initial):
        stat = path.stat()
        row = self.state.one("SELECT * FROM files WHERE source=? AND path=?", (source.id, relative))
        if row is None:
            status = "IGNORED" if initial and source.initial_scan == "new_only" else "STABILIZING"
            self.state.db.execute("INSERT INTO files(source,path,size,mtime,changed,status) VALUES (?,?,?,?,?,?)",
                                  (source.id, relative, stat.st_size, stat.st_mtime_ns, now, status))
        elif (row["size"], row["mtime"]) != (stat.st_size, stat.st_mtime_ns) or row["status"] == "MISSING":
            self.state.db.execute("UPDATE files SET size=?,mtime=?,changed=?,stable_at=NULL,status='STABILIZING',error=NULL WHERE source=? AND path=?",
                                  (stat.st_size, stat.st_mtime_ns, now, source.id, relative))
        row = self.state.one("SELECT * FROM files WHERE source=? AND path=?", (source.id, relative))
        if row["status"] in ("IGNORED", "BATCHED") or now - row["changed"] < source.stable_seconds:
            return
        self._snapshot(source, row, now)

    def _snapshot(self, source, row, now):
        if shutil.disk_usage(self.config.agent.work_dir).free < row["size"] + self.config.agent.min_free_bytes:
            raise OSError("insufficient spool space")
        path = source.root / row["path"]
        capture_id = uuid.uuid4().hex
        directory = self.config.agent.work_dir / "spool" / capture_id
        directory.mkdir(parents=True)
        try:
            self._write_snapshot(source, row, now, capture_id, directory)
        finally:
            # 仅删除本次尝试中尚未注册的文件，绝不删除已发布的快照。
            if not self.state.one("SELECT id FROM batches WHERE id=?", (capture_id,)):
                for name in ("file.tmp", "file.data"):
                    (directory / name).unlink(missing_ok=True)
                if directory.exists():
                    directory.rmdir()

    def _write_snapshot(self, source, row, now, capture_id, directory):
        path = source.root / row["path"]
        temporary, destination = directory / "file.tmp", directory / "file.data"
        before = path.stat()
        if (before.st_size, before.st_mtime_ns) != (row["size"], row["mtime"]):
            return
        with path.open("rb") as src, temporary.open("wb") as dst:
            for block in iter(lambda: src.read(1024 * 1024), b""):
                self._check_stop()
                dst.write(block)
            dst.flush()
            os.fsync(dst.fileno())
        after = path.stat()
        if (after.st_size, after.st_mtime_ns) != (before.st_size, before.st_mtime_ns):
            temporary.unlink()
            self.state.db.execute("UPDATE files SET size=?,mtime=?,changed=?,status='STABILIZING' WHERE source=? AND path=?",
                                  (after.st_size, after.st_mtime_ns, now, source.id, row["path"]))
            return
        os.replace(temporary, destination)
        size, digest = digest_file(destination, self._check_stop)
        if size != row["size"]:
            raise ValueError("snapshot size mismatch")
        if row["batch"]:
            previous = json.loads(self.state.one("SELECT manifest FROM batches WHERE id=?", (row["batch"],))[0])["files"][0]
            if previous["checksum"]["value"] == digest:
                self.state.db.execute("UPDATE files SET status='BATCHED',error=NULL WHERE source=? AND path=?", (source.id, row["path"]))
                return
        descriptor = {"target_key": safe_key(row["path"]), "size": size,
                      "content_type": {".hdf": "application/x-hdf", ".h5": "application/x-hdf", ".xml": "application/xml"}.get(path.suffix.lower(), "application/octet-stream"),
                      "checksum": {"algorithm": "sha256", "value": digest}}
        self.state.register_file(source.id, row["path"], capture_id, descriptor, destination, now)
