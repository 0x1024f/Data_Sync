"""Polling inventory, batch quiet windows and immutable disk snapshots."""
import hashlib
import json
import logging
import os
import re
import shutil
from datetime import datetime
from pathlib import Path, PurePosixPath

from .config import safe_key
from .manifest import build_manifest, canonical, digest_file, utc_text

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
        self.config, self.state = config, state
        self.stop = stop

    def _check_stop(self):
        if self.stop and self.stop.is_set():
            raise InterruptedError("stopping")

    def scan(self, source, now):
        if not source.root.is_dir():
            raise OSError("source directory unavailable")
        initial = not self.state.one("SELECT initialized FROM sources WHERE id=?", (source.id,))[0]
        seen = set()
        # os.walk onerror is deliberately fatal: an incomplete scan must not seal batches.
        def fail(error):
            raise error
        for directory, dirs, names in os.walk(source.root, onerror=fail, followlinks=False):
            dirs[:] = sorted(d for d in dirs if not (Path(directory) / d).is_symlink()) if source.recursive else []
            for name in sorted(names):
                self._check_stop()
                path = Path(directory) / name
                if path.is_symlink() or not path.is_file():
                    continue
                relative = path.relative_to(source.root).as_posix()
                if not matches(relative, source.include) or matches(relative, source.exclude):
                    continue
                safe_key(relative)
                seen.add(relative)
                stat = path.stat()
                row = self.state.one("SELECT * FROM files WHERE source=? AND path=?", (source.id, relative))
                if row is None:
                    status = "IGNORED" if initial and source.initial_scan == "new_only" else "DISCOVERED"
                    self.state.db.execute("INSERT INTO files(source,path,size,mtime,changed,status) VALUES (?,?,?,?,?,?)",
                                          (source.id, relative, stat.st_size, stat.st_mtime_ns, now, status))
                    row = self.state.one("SELECT * FROM files WHERE source=? AND path=?", (source.id, relative))
                if source.path_layout == "relative" and relative.split("/")[0] == "_data_sync":
                    if row["status"] != "QUARANTINED":
                        self._error(source.id, relative, "RESERVED_PATH")
                    continue
                if row["status"] in ("IGNORED", "QUARANTINED"):
                    continue
                changed = row["size"] != stat.st_size or row["mtime"] != stat.st_mtime_ns
                if row["status"] == "BATCHED":
                    if changed:
                        self._error(source.id, relative, "SOURCE_CHANGED_AFTER_SEAL")
                    continue
                if row["batch"]:
                    parent = self.state.one("SELECT status FROM batches WHERE id=?", (row["batch"],))
                    if parent[0] != "OPEN":
                        # Preserve sealed inventory across crashes. A subsequent
                        # snapshot checks original stats instead of adopting edits.
                        continue
                if changed:
                    self.state.db.execute("UPDATE files SET size=?,mtime=?,changed=?,stable_at=NULL,status='STABILIZING',error=NULL WHERE source=? AND path=?",
                                          (stat.st_size, stat.st_mtime_ns, now, source.id, relative))
                    row = self.state.one("SELECT * FROM files WHERE source=? AND path=?", (source.id, relative))
                if not row["batch"]:
                    self._assign(source, row, now)
                    row = self.state.one("SELECT * FROM files WHERE source=? AND path=?", (source.id, relative))
                if row["status"] == "QUARANTINED":
                    continue
                if now - row["changed"] >= source.stable_seconds:
                    if row["status"] != "STABLE":
                        self.state.db.execute("UPDATE files SET status='STABLE',stable_at=?,error=NULL WHERE source=? AND path=?", (now, source.id, relative))
                        self.state.db.execute("UPDATE batches SET touched=? WHERE id=? AND status='OPEN'", (now, row["batch"]))
                else:
                    self.state.db.execute("UPDATE files SET status='STABILIZING' WHERE source=? AND path=?", (source.id, relative))
                if changed:
                    self.state.db.execute("UPDATE batches SET touched=? WHERE id=? AND status='OPEN'", (now, row["batch"]))
        self.state.db.execute("UPDATE sources SET initialized=1 WHERE id=?", (source.id,))
        for row in self.state.all("SELECT path FROM files WHERE source=? AND status IN ('DISCOVERED','STABILIZING','STABLE')", (source.id,)):
            if row["path"] not in seen:
                self.state.db.execute("UPDATE files SET status='STABILIZING',error='SOURCE_MISSING',changed=? WHERE source=? AND path=?", (now, source.id, row["path"]))
        for batch in self.state.all("SELECT * FROM batches WHERE source=? AND status='OPEN'", (source.id,)):
            pending = self.state.one("SELECT count(*) FROM files WHERE batch=? AND status!='STABLE'", (batch["id"],))[0]
            if not pending and now - batch["touched"] >= source.quiet_seconds:
                self.state.db.execute("UPDATE batches SET status='SEALED' WHERE id=?", (batch["id"],))
        for batch in self.state.all("SELECT * FROM batches WHERE source=? AND status='SEALED'", (source.id,)):
            self._snapshot(source, batch)

    def _error(self, source, path, code):
        self.state.db.execute("UPDATE files SET status='QUARANTINED',error=? WHERE source=? AND path=?", (code, source, path))
        log.error("file_quarantined", extra={"source": source, "code": code})

    def _assign(self, source, row, now):
        match = re.fullmatch(source.filename_regex, Path(row["path"]).name)
        if not match:
            self._error(source.id, row["path"], "NAME_MISMATCH")
            return
        groups = {k: v for k, v in match.groupdict().items() if v is not None}
        batch_no = groups["batch_no"]
        dataset = {"type": "satellite_file"}
        try:
            safe_key(batch_no)
            if "/" in batch_no:
                raise ValueError("batch number must be a single component")
            for key in ("satellite", "sensor", "level"):
                if key in groups:
                    dataset[key] = groups[key]
            if "obs_time" in groups:
                parsed = datetime.strptime(groups["obs_time"], source.obs_time_format)
                dataset["obs_time"] = (parsed if parsed.utcoffset() is not None else datetime.fromisoformat(parsed.isoformat() + source.timezone_offset)).isoformat()
        except ValueError:
            self._error(source.id, row["path"], "METADATA_INVALID")
            return
        old = self.state.one("SELECT * FROM batches WHERE source=? AND batch_no=?", (source.id, batch_no))
        if old and old["status"] != "OPEN":
            self._error(source.id, row["path"], "LATE_FILE")
            return
        if old and json.loads(old["dataset"]) != dataset:
            self._error(source.id, row["path"], "BATCH_METADATA_CONFLICT")
            self.state.db.execute("UPDATE batches SET status='QUARANTINED',error='BATCH_METADATA_CONFLICT' WHERE id=?", (old["id"],))
            return
        batch_id = old["id"] if old else self.state.create_batch(source.id, batch_no, "file", dataset, now)
        self.state.db.execute("UPDATE files SET batch=? WHERE source=? AND path=?", (batch_id, source.id, row["path"]))
        self.state.db.execute("UPDATE batches SET touched=? WHERE id=?", (now, batch_id))

    def _snapshot(self, source, batch):
        rows = self.state.all("SELECT * FROM files WHERE batch=? ORDER BY path", (batch["id"],))
        if shutil.disk_usage(self.config.agent.work_dir).free < sum(r["size"] for r in rows) + self.config.agent.min_free_bytes:
            raise OSError("insufficient spool space")
        dataset = json.loads(batch["dataset"])
        if source.path_layout == "relative":
            dataset["capture_id"] = batch["id"]
        date = datetime.fromisoformat(dataset.get("obs_time", utc_text(batch["created"]))).strftime("%Y/%m/%d")
        prefix = "/".join((self.config.targets[0].prefix, source.prefix, self.config.agent.source_id, source.id, date, batch["batch_no"]))
        directory = self.config.agent.work_dir / "spool" / batch["id"]
        files, local = [], {}
        for row in rows:
            self._check_stop()
            path = source.root / row["path"]
            destination = directory / (hashlib.sha256(row["path"].encode()).hexdigest() + ".data")
            if not destination.exists():
                before = path.stat()
                if (before.st_size, before.st_mtime_ns) != (row["size"], row["mtime"]):
                    self.state.db.execute("UPDATE batches SET status='QUARANTINED',error='SOURCE_CHANGED_DURING_SEAL' WHERE id=?", (batch["id"],))
                    return
                destination.parent.mkdir(parents=True, exist_ok=True)
                temporary = destination.with_suffix(".tmp")
                with path.open("rb") as src, temporary.open("wb") as dst:
                    for block in iter(lambda: src.read(1024 * 1024), b""):
                        self._check_stop()
                        dst.write(block)
                    dst.flush()
                    os.fsync(dst.fileno())
                after = path.stat()
                if (after.st_size, after.st_mtime_ns) != (before.st_size, before.st_mtime_ns):
                    self.state.db.execute("UPDATE batches SET status='QUARANTINED',error='SOURCE_CHANGED_DURING_SEAL' WHERE id=?", (batch["id"],))
                    return
                os.replace(temporary, destination)
            size, digest = digest_file(destination, self._check_stop)
            if size != row["size"]:
                raise ValueError("snapshot size mismatch")
            groups = re.fullmatch(source.filename_regex, Path(row["path"]).name).groupdict()
            key = safe_key(row["path"] if source.path_layout == "relative" else prefix + "/data/" + row["path"])
            files.append({"name": Path(row["path"]).name, "role": groups.get("role") or "primary", "relative_path": row["path"],
                          "target_key": key, "size": size, "content_type": {".hdf": "application/x-hdf", ".h5": "application/x-hdf", ".xml": "application/xml"}.get(Path(row["path"]).suffix.lower(), "application/octet-stream"),
                          "checksum": {"algorithm": "sha256", "value": digest}})
            local[key] = str(destination)
        manifest = build_manifest({"source_id": self.config.agent.source_id, "system": source.system, "relative_path": "."},
                                  batch["batch_no"], dataset, files, utc_text(batch["created"]))
        atomic_bytes(directory / "manifest.json", canonical(manifest))
        manifest_key = ("_data_sync/manifests/" + manifest["manifest_id"].removeprefix("sha256:") + ".json"
                        if source.path_layout == "relative" else prefix + "/manifest.json")
        self.state.ready(batch["id"], manifest, manifest_key, local)
