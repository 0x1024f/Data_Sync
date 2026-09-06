"""SQLite durable ledger; each worker uses its own connection."""
import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

from .manifest import canonical

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
"""


class State:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(path), timeout=30, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        if self.db.execute("PRAGMA user_version").fetchone()[0] > 2:
            raise ValueError("state database requires newer agent")
        self.db.executescript(SCHEMA)
        with self.transaction() as db:
            if "part_size" not in [r[1] for r in db.execute("PRAGMA table_info(uploads)")]:
                db.execute("ALTER TABLE uploads ADD COLUMN part_size INTEGER NOT NULL DEFAULT 0")
            db.execute("PRAGMA user_version=2")

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
                # Capture/identity changes need a new source id; tuning may change.
                fields = ("root", "system", "filename_regex", "obs_time_format", "timezone_offset", "prefix", "include", "exclude", "recursive") if hasattr(source, "root") else ("host", "port", "database", "table", "primary_key", "fields", "prefix")
                fingerprint = canonical({k: str(getattr(source, k)) for k in fields}).decode()
                old = self.one("SELECT fingerprint FROM sources WHERE id=?", (source.id,))
                if old and old[0] != fingerprint:
                    raise ValueError("source identity changed; use a new source id")
                db.execute("INSERT OR IGNORE INTO sources(id,fingerprint) VALUES (?,?)", (source.id, fingerprint))
            configured = {t.id for t in config.targets}
            for old in self.all("SELECT id FROM targets"):
                if old[0] not in configured:
                    db.execute("UPDATE targets SET enabled=0 WHERE id=?", (old[0],))
            for target in config.targets:
                fingerprint = canonical([target.host, target.port, target.bucket, target.prefix]).decode()
                old = self.one("SELECT * FROM targets WHERE id=?", (target.id,))
                if old and old["fingerprint"] != fingerprint:
                    raise ValueError("target endpoint changed; use a new target id")
                requested = target.enabled_at.timestamp() if target.enabled_at else now
                if old is None:
                    db.execute("INSERT INTO targets VALUES (?,?,?,?)", (target.id, fingerprint, target.enabled, max(now, requested)))
                else:
                    activation = max(now, requested) if target.enabled and not old["enabled"] else old["activation"]
                    db.execute("UPDATE targets SET enabled=?,activation=? WHERE id=?", (target.enabled, activation, target.id))
            # A stopped process's leases expire; a process lock prevents simultaneous owners.
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

    def claim(self, target, owner, lease_seconds=1800):
        now = time.time()
        with self.transaction() as db:
            row = self.one("""SELECT t.* FROM tasks t JOIN batches b ON b.id=t.batch JOIN targets d ON d.id=t.target
                WHERE t.target=? AND d.enabled=1 AND b.status='READY'
                AND t.status NOT IN ('COMMITTED','BLOCKED') AND t.next_retry<=?
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
                "batches": [dict(r) for r in self.all("SELECT status,count(*) AS count FROM batches GROUP BY status")],
                "file_errors": [dict(r) for r in self.all("SELECT source,error,count(*) AS count FROM files WHERE error IS NOT NULL GROUP BY source,error")],
                "checkpoints": [dict(r) for r in self.all("SELECT * FROM checkpoints")],
                "runtime": [dict(r) for r in self.all("SELECT * FROM settings WHERE key!='source_id'")]}
