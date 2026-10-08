"""基于 SQLite 的持久化台账；每个工作线程使用独立连接。"""
import json
import sqlite3
import time
import uuid
from contextlib import closing, contextmanager
from pathlib import Path

from .manifest import canonical


class LegacyFileStateError(ValueError):
    """必须保留旧的文件分组台账并创建新台账，不能直接迁移。"""


class ObjectKeyConflict(ValueError):
    """此目标对象键已被另一个数据源占用。"""

SCHEMA = """
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sources (id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, initialized INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS targets (id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, enabled INTEGER NOT NULL, activation REAL NOT NULL);
CREATE TABLE IF NOT EXISTS files (
 source TEXT NOT NULL, path TEXT NOT NULL, size INTEGER NOT NULL, mtime INTEGER NOT NULL,
 changed REAL NOT NULL, stable_at REAL, batch TEXT, status TEXT NOT NULL, error TEXT,
 PRIMARY KEY(source,path));
CREATE TABLE IF NOT EXISTS batches (
 id TEXT PRIMARY KEY, source TEXT NOT NULL, batch_no TEXT NOT NULL, kind TEXT NOT NULL,
 status TEXT NOT NULL, created REAL NOT NULL, touched REAL NOT NULL, dataset TEXT NOT NULL,
 manifest TEXT, manifest_key TEXT, local_files TEXT, error TEXT,
 UNIQUE(source,batch_no));
CREATE TABLE IF NOT EXISTS tasks (
 id INTEGER PRIMARY KEY, batch TEXT NOT NULL REFERENCES batches(id), target TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'PENDING', attempts INTEGER NOT NULL DEFAULT 0,
 next_retry REAL NOT NULL DEFAULT 0, owner TEXT, lease_until REAL, error TEXT,
 UNIQUE(batch,target));
CREATE INDEX IF NOT EXISTS task_queue ON tasks(target,status,next_retry);
CREATE TABLE IF NOT EXISTS attempts (
 id INTEGER PRIMARY KEY, task INTEGER NOT NULL, started REAL NOT NULL, ended REAL,
 outcome TEXT, error TEXT);
CREATE TABLE IF NOT EXISTS uploads (
 task INTEGER NOT NULL, key TEXT NOT NULL, upload_id TEXT NOT NULL,
 parts TEXT NOT NULL DEFAULT '{}', part_size INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(task,key));
CREATE TABLE IF NOT EXISTS checkpoints (source TEXT PRIMARY KEY, cursor INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS object_owners (
 target TEXT NOT NULL, key TEXT NOT NULL, owner TEXT NOT NULL, PRIMARY KEY(target,key));
"""


class State:
    def __init__(self, path: Path):
        if path.exists():
            with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as probe:
                version = probe.execute("PRAGMA user_version").fetchone()[0]
                if version > 3:
                    raise ValueError("state database requires newer agent")
                tables = {r[0] for r in probe.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if version < 3 and "sources" in tables:
                    if any("root" in json.loads(r[0]) for r in probe.execute("SELECT fingerprint FROM sources")):
                        raise LegacyFileStateError("legacy file state: configure a new work_dir; retain the old directory")
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(path), timeout=30, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        if self.db.execute("PRAGMA user_version").fetchone()[0] > 3:
            raise ValueError("state database requires newer agent")
        self.db.executescript(SCHEMA)
        with self.transaction() as db:
            if "part_size" not in [r[1] for r in db.execute("PRAGMA table_info(uploads)")]:
                db.execute("ALTER TABLE uploads ADD COLUMN part_size INTEGER NOT NULL DEFAULT 0")
            if "object_key" not in [r[1] for r in db.execute("PRAGMA table_info(batches)")]:
                db.execute("ALTER TABLE batches ADD COLUMN object_key TEXT")
            db.execute("CREATE INDEX IF NOT EXISTS file_object_queue ON batches(object_key,kind)")
            db.execute("PRAGMA user_version=3")

    def close(self):
        self.db.close()

    def one(self, sql, args=()):
        return self.db.execute(sql, args).fetchone()

    def all(self, sql, args=()):
        return self.db.execute(sql, args).fetchall()

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield self.db
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def configure(self, config, now=None):
        now = time.time() if now is None else now
        with self.transaction() as db:
            source_id = self.one("SELECT value FROM settings WHERE key='source_id'")
            if source_id and source_id[0] != config.agent.source_id:
                raise ValueError("source_id cannot change for existing state")
            db.execute("INSERT OR IGNORE INTO settings VALUES ('source_id',?)", (config.agent.source_id,))
            for source in config.files + config.mysql:
                # 采集规则或身份信息变更时需要新的数据源 ID；调优参数可以修改。
                fields = ("root", "system", "include", "exclude", "recursive") if hasattr(source, "root") else ("host", "port", "database", "table", "primary_key", "fields", "prefix")
                identity = {k: str(getattr(source, k)) for k in fields}
                fingerprint = canonical(identity).decode()
                old = self.one("SELECT fingerprint FROM sources WHERE id=?", (source.id,))
                previous = json.loads(old[0]) if old else None
                if old and previous != identity:
                    raise ValueError("source identity changed; use a new source id")
                db.execute("INSERT OR IGNORE INTO sources(id,fingerprint) VALUES (?,?)", (source.id, fingerprint))
            configured = {t.id for t in config.targets}
            for old in self.all("SELECT id FROM targets"):
                if old[0] not in configured:
                    db.execute("UPDATE targets SET enabled=0 WHERE id=?", (old[0],))
            for target in config.targets:
                fingerprint = canonical([target.host, target.port, target.bucket, target.prefix]).decode()
                # 保留旧版 HTTPS 指纹的兼容性，同时区分 HTTP 端点。
                if target.scheme != "https":
                    fingerprint = canonical([target.host, target.port, target.bucket, target.prefix, target.scheme]).decode()
                old = self.one("SELECT * FROM targets WHERE id=?", (target.id,))
                if old and old["fingerprint"] != fingerprint:
                    raise ValueError("target endpoint changed; use a new target id")
                requested = target.enabled_at.timestamp() if target.enabled_at else now
                if old is None:
                    db.execute("INSERT INTO targets VALUES (?,?,?,?)", (target.id, fingerprint, target.enabled, max(now, requested)))
                else:
                    activation = max(now, requested) if target.enabled and not old["enabled"] else old["activation"]
                    db.execute("UPDATE targets SET enabled=?,activation=? WHERE id=?", (target.enabled, activation, target.id))
            # 已停止进程的租约会过期；进程锁可防止多个进程同时持有所有权。
            db.execute("UPDATE tasks SET owner=NULL,lease_until=NULL WHERE status!='COMMITTED'")

    def create_batch(self, source, batch_no, kind, dataset, now):
        batch_id = uuid.uuid4().hex
        with self.transaction() as db:
            db.execute("INSERT INTO batches(id,source,batch_no,kind,status,created,touched,dataset) VALUES (?,?,?,?,'OPEN',?,?,?)",
                       (batch_id, source, batch_no, kind, now, now, canonical(dataset).decode()))
            for t in self.all("SELECT id FROM targets WHERE enabled=1 AND activation<=?", (now,)):
                db.execute("INSERT INTO tasks(batch,target) VALUES (?,?)", (batch_id, t[0]))
        return batch_id

    def ready(self, batch_id, manifest, key, local_files, checkpoint=None):
        with self.transaction() as db:
            db.execute("UPDATE batches SET status='READY',manifest=?,manifest_key=?,local_files=? WHERE id=?",
                       (canonical(manifest).decode(), key, canonical(local_files).decode(), batch_id))
            db.execute("UPDATE files SET status='BATCHED' WHERE batch=?", (batch_id,))
            if checkpoint:
                db.execute("INSERT INTO checkpoints VALUES (?,?) ON CONFLICT(source) DO UPDATE SET cursor=excluded.cursor", checkpoint)

    def register_file(self, source, relative, capture_id, descriptor, local, now):
        with self.transaction() as db:
            db.execute("""INSERT INTO batches(id,source,batch_no,kind,status,created,touched,dataset,
                manifest,local_files,object_key) VALUES (?,?,?,'file','READY',?,?,'{}',?,?,?)""",
                (capture_id, source, capture_id, now, now, canonical({"files": [descriptor]}).decode(),
                 canonical({relative: str(local)}).decode(), relative))
            for target in self.all("SELECT id FROM targets WHERE enabled=1 AND activation<=?", (now,)):
                db.execute("INSERT INTO tasks(batch,target) VALUES (?,?)", (capture_id, target[0]))
            db.execute("UPDATE files SET batch=?,status='BATCHED',error=NULL WHERE source=? AND path=?",
                       (capture_id, source, relative))

    def reserve_keys(self, task, batch, keys):
        owner = ("file:" + batch["source"] + ":" + batch["object_key"]
                 if batch["kind"] == "file" else "mysql:" + batch["id"])
        with self.transaction() as db:
            for key in keys:
                row = self.one("SELECT owner FROM object_owners WHERE target=? AND key=?", (task["target"], key))
                if row and row[0] != owner:
                    raise ObjectKeyConflict("another source owns this object key")
            for key in keys:
                db.execute("INSERT OR IGNORE INTO object_owners VALUES (?,?,?)", (task["target"], key, owner))

    def claim(self, target, owner, lease_seconds=1800):
        now = time.time()
        with self.transaction() as db:
            row = self.one("""SELECT t.* FROM tasks t JOIN batches b ON b.id=t.batch JOIN targets d ON d.id=t.target
                WHERE t.target=? AND d.enabled=1 AND b.status='READY'
                AND t.status NOT IN ('COMMITTED','BLOCKED') AND t.next_retry<=?
                AND (b.kind!='file' OR NOT EXISTS (
                    SELECT 1 FROM tasks older JOIN batches ob ON ob.id=older.batch
                    WHERE older.target=t.target AND older.id<t.id AND ob.kind='file'
                    AND ob.object_key=b.object_key AND older.status!='COMMITTED'))
                AND (t.owner IS NULL OR t.lease_until<?) ORDER BY t.id LIMIT 1""", (target, now, now))
            if not row:
                return None
            db.execute("UPDATE tasks SET owner=?,lease_until=?,attempts=attempts+1 WHERE id=?", (owner, now + lease_seconds, row["id"]))
            db.execute("INSERT INTO attempts(task,started) VALUES (?,?)", (row["id"], now))
            return dict(self.one("SELECT * FROM tasks WHERE id=?", (row["id"],)))

    def phase(self, task, phase):
        self.db.execute("UPDATE tasks SET status=?,lease_until=? WHERE id=? AND owner=?",
                        (phase, time.time() + 1800, task["id"], task["owner"]))

    def finish(self, task, status, error=None, delay=0):
        with self.transaction() as db:
            db.execute("UPDATE tasks SET status=?,error=?,next_retry=?,owner=NULL,lease_until=NULL WHERE id=? AND owner=?",
                       (status, error, time.time() + delay, task["id"], task["owner"]))
            db.execute("UPDATE attempts SET ended=?,outcome=?,error=? WHERE task=? AND ended IS NULL", (time.time(), status, error, task["id"]))

    def health(self):
        return {"tasks": [dict(r) for r in self.all("SELECT target,status,count(*) AS count FROM tasks GROUP BY target,status")],
                "file_versions": [dict(r) for r in self.all("SELECT status,count(*) AS count FROM batches WHERE kind='file' GROUP BY status")],
                "batches": [dict(r) for r in self.all("SELECT status,count(*) AS count FROM batches WHERE kind!='file' GROUP BY status")],
                "file_errors": [dict(r) for r in self.all("SELECT source,error,count(*) AS count FROM files WHERE error IS NOT NULL GROUP BY source,error")],
                "checkpoints": [dict(r) for r in self.all("SELECT * FROM checkpoints")],
                "runtime": [dict(r) for r in self.all("SELECT * FROM settings WHERE key!='source_id'")]}
