"""Insert-only capture. Requires commit-monotonic integer primary keys."""
import base64
import hashlib
import json
import os
import shutil
from datetime import date, datetime, time as dt_time, timedelta
from decimal import Decimal

from .config import secret
from .files import atomic_bytes
from .manifest import build_manifest, canonical, digest_file, utc_text


def encode_value(value):
    if isinstance(value, datetime):
        # MySQL DATETIME is a wall clock, not an observation timestamp.
        return {"$type": "datetime", "value": value.isoformat()}
    if isinstance(value, (date, dt_time)):
        return {"$type": type(value).__name__, "value": value.isoformat()}
    if isinstance(value, timedelta):
        return {"$type": "duration_us", "value": (value.days * 86400 + value.seconds) * 1000000 + value.microseconds}
    if isinstance(value, Decimal):
        return {"$type": "decimal", "value": str(value)}
    if isinstance(value, bytes):
        return {"$type": "binary", "encoding": "base64", "value": base64.b64encode(value).decode("ascii")}
    if value is None or isinstance(value, (str, int, bool, float)):
        return value
    raise TypeError("unsupported database value type")


def connect(source):
    import pymysql
    options = dict(host=source.host, port=source.port, user=secret(source.user), password=secret(source.password),
                   database=source.database, charset="utf8mb4", autocommit=True,
                   cursorclass=pymysql.cursors.DictCursor, connect_timeout=10, read_timeout=60, write_timeout=60)
    if source.ca_bundle:
        options.update(ssl_ca=str(source.ca_bundle), ssl_verify_cert=True, ssl_verify_identity=True)
    return pymysql.connect(**options)


class MySQLCollector:
    def __init__(self, config, state, connector=connect):
        self.config, self.state, self.connector = config, state, connector
        self.validated = set()

    def poll(self, source, now):
        self.recover_prepared(source)
        # Publish a prepared batch before performing any new SELECT. Its exact rows
        # and target membership survive crashes before SQLite checkpoint commit.
        pending = self.state.one("SELECT * FROM batches WHERE source=? AND status='SEALED'", (source.id,))
        if pending:
            return self._publish(source, pending)
        checkpoint = self.state.one("SELECT cursor FROM checkpoints WHERE source=?", (source.id,))
        connection = self.connector(source)
        try:
            with connection.cursor() as cursor:
                if source.id not in self.validated:
                    cursor.execute("SELECT COLUMN_NAME,DATA_TYPE,EXTRA FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s", (source.database, source.table))
                    columns = {r["COLUMN_NAME"]: r for r in cursor.fetchall()}
                    cursor.execute("SELECT COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND INDEX_NAME='PRIMARY' ORDER BY SEQ_IN_INDEX", (source.database, source.table))
                    primary = [r["COLUMN_NAME"] for r in cursor.fetchall()]
                    info = columns.get(source.primary_key, {})
                    if primary != [source.primary_key] or info.get("DATA_TYPE") not in ("tinyint", "smallint", "mediumint", "int", "bigint") or "auto_increment" not in info.get("EXTRA", "") or not set(source.fields).issubset(columns):
                        raise ValueError("source requires a single auto-increment integer primary key and existing fields")
                    self.validated.add(source.id)
                if checkpoint is None:
                    start = 0
                    if source.initial_scan == "new_only":
                        cursor.execute(f"SELECT COALESCE(MAX(`{source.primary_key}`),0) AS high FROM `{source.table}`")
                        start = cursor.fetchone()["high"]
                    self._valid_id(start, allow_zero=True)
                    self.state.db.execute("INSERT INTO checkpoints VALUES (?,?)", (source.id, start))
                else:
                    start = checkpoint[0]
                columns = ",".join(f"`{f}`" for f in source.fields)
                cursor.execute(f"SELECT {columns} FROM `{source.table}` WHERE `{source.primary_key}` > %s ORDER BY `{source.primary_key}` LIMIT %s", (start, source.batch_size))
                rows = cursor.fetchall()
        finally:
            connection.close()
        if not rows:
            return
        previous = start
        lines = []
        for row in rows:
            value = row[source.primary_key]
            self._valid_id(value)
            if value <= previous:
                raise ValueError("database cursor must increase strictly")
            previous = value
            lines.append(canonical({f: encode_value(row[f]) for f in source.fields}) + b"\n")
        data = b"".join(lines)
        if shutil.disk_usage(self.config.agent.work_dir).free < len(data) + self.config.agent.min_free_bytes:
            raise OSError("insufficient spool space")
        batch_no = f"{source.id}-{start}-{previous}"
        # Deterministic staging path: recover the prepared envelope before SELECT on
        # the next poll, even when a crash occurs before registering the batch.
        prepared = self.config.agent.work_dir / "prepared" / (source.id + ".json")
        if not prepared.exists():
            dataset = {"type": "database_increment", "database": source.database, "table": source.table,
                       "operation": "insert", "primary_key": source.primary_key, "encoding": "tagged-jsonl-v1",
                       "cursor": {"type": "primary_key", "start_exclusive": start, "end_inclusive": previous}, "record_count": len(rows)}
            token = hashlib.sha256(f"{source.id}:{start}:{previous}".encode()).hexdigest()
            path = self.config.agent.work_dir / "spool" / token / "rows.jsonl"
            atomic_bytes(path, data)
            targets = [t[0] for t in self.state.all("SELECT id FROM targets WHERE enabled=1 AND activation<=?", (now,))]
            atomic_bytes(prepared, canonical({"batch_no": batch_no, "dataset": dataset, "path": str(path), "created": now, "targets": targets}))
        self.recover_prepared(source)

    def recover_prepared(self, source):
        prepared = self.config.agent.work_dir / "prepared" / (source.id + ".json")
        if not prepared.exists():
            return
        envelope = json.loads(prepared.read_text(encoding="utf-8"))
        old = self.state.one("SELECT * FROM batches WHERE source=? AND batch_no=?", (source.id, envelope["batch_no"]))
        if not old:
            import uuid
            bid = uuid.uuid4().hex
            with self.state.transaction() as db:
                db.execute("INSERT INTO batches(id,source,batch_no,kind,status,created,touched,dataset,local_files) VALUES (?,?,?,'mysql','SEALED',?,?,?,?)",
                           (bid, source.id, envelope["batch_no"], envelope["created"], envelope["created"], canonical(envelope["dataset"]).decode(), envelope["path"]))
                for target in envelope["targets"]:
                    db.execute("INSERT INTO tasks(batch,target) VALUES (?,?)", (bid, target))
            old = self.state.one("SELECT * FROM batches WHERE id=?", (bid,))
        if old["status"] == "SEALED":
            self._publish(source, old)
        # Recoverable envelope is removed only after ledger/checkpoint commit.
        prepared.unlink()

    def _publish(self, source, batch):
        from pathlib import Path
        path = Path(batch["local_files"])
        dataset = json.loads(batch["dataset"])
        size, digest = digest_file(path)
        prefix = "/".join((self.config.targets[0].prefix, source.prefix, self.config.agent.source_id, source.id, batch["batch_no"]))
        key = prefix + "/data/rows.jsonl"
        files = [{"name": "rows.jsonl", "role": "primary", "relative_path": "rows.jsonl", "target_key": key,
                  "size": size, "content_type": "application/x-ndjson", "checksum": {"algorithm": "sha256", "value": digest}}]
        manifest = build_manifest({"source_id": self.config.agent.source_id, "system": source.database}, batch["batch_no"], dataset, files, utc_text(batch["created"]))
        atomic_bytes(path.parent / "manifest.json", canonical(manifest))
        self.state.ready(batch["id"], manifest, prefix + "/manifest.json", {key: str(path)},
                         (source.id, dataset["cursor"]["end_inclusive"]))

    @staticmethod
    def _valid_id(value, allow_zero=False):
        if type(value) is not int or value < (0 if allow_zero else 1) or value > 9223372036854775807:
            raise ValueError("cursor must be a positive signed 64-bit integer")
