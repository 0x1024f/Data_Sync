"""Immutable, canonical transport contract shared by all destinations."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from .config import safe_key


def utc_text(timestamp=None):
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat() if timestamp is not None else datetime.now(timezone.utc).isoformat()


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest_file(path: Path, progress=None):
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
            size += len(block)
            if progress:
                progress()
    return size, digest.hexdigest()


def identity(manifest):
    payload = {k: manifest[k] for k in ("schema_version", "batch_no", "dataset")}
    payload["source_id"] = manifest["source"]["source_id"]
    payload["files"] = sorted(manifest["files"], key=lambda f: f["target_key"])
    return "sha256:" + hashlib.sha256(canonical(payload)).hexdigest()


def build_manifest(source, batch_no, dataset, files, created_at):
    value = dict(schema_version="1.0", batch_no=batch_no, source=source, dataset=dataset,
                 files=sorted(files, key=lambda f: f["target_key"]), created_at=created_at,
                 summary={"file_count": len(files), "total_size": sum(f["size"] for f in files)})
    value["manifest_id"] = identity(value)
    validate_manifest(value)
    return value


def validate_manifest(value):
    expected = {"schema_version", "manifest_id", "batch_no", "source", "dataset", "files", "summary", "created_at"}
    if set(value) != expected:
        raise ValueError("invalid manifest fields")
    if value["schema_version"] != "1.0" or not value["files"]:
        raise ValueError("unsupported or empty manifest")
    if not isinstance(value["batch_no"], str) or not value["batch_no"]:
        raise ValueError("batch_no is required")
    for key in ("source_id", "system"):
        if not isinstance(value["source"].get(key), str) or not value["source"][key]:
            raise ValueError("invalid source identity")
    dataset = value["dataset"]
    if dataset.get("type") not in ("satellite_file", "database_increment"):
        raise ValueError("unknown dataset type")
    if "obs_time" in dataset and datetime.fromisoformat(dataset["obs_time"]).utcoffset() is None:
        raise ValueError("obs_time requires timezone")
    if dataset["type"] == "database_increment":
        if any(k not in dataset for k in ("database", "table", "primary_key", "cursor", "record_count", "encoding", "operation")):
            raise ValueError("incomplete database dataset")
        cursor = dataset["cursor"]
        if cursor.get("type") != "primary_key" or not (0 <= cursor["start_exclusive"] < cursor["end_inclusive"]) or type(dataset["record_count"]) is not int or dataset["record_count"] <= 0:
            raise ValueError("invalid database cursor/count")
        if dataset["encoding"] != "tagged-jsonl-v1" or dataset["operation"] != "insert":
            raise ValueError("unsupported database encoding/operation")
    if value["manifest_id"] != identity(value):
        raise ValueError("manifest identity mismatch")
    if datetime.fromisoformat(value["created_at"]).utcoffset() is None:
        raise ValueError("created_at requires timezone")
    keys = set()
    for f in value["files"]:
        if set(f) != {"name", "role", "relative_path", "target_key", "size", "content_type", "checksum"}:
            raise ValueError("invalid file fields")
        if any(not isinstance(f[k], str) or not f[k] for k in ("name", "role", "content_type")):
            raise ValueError("invalid file metadata")
        safe_key(f["target_key"])
        safe_key(f["relative_path"])
        if f["target_key"] in keys or type(f["size"]) is not int or f["size"] < 0:
            raise ValueError("duplicate key or invalid size")
        keys.add(f["target_key"])
        c = f["checksum"]
        if c["algorithm"] != "sha256" or len(c["value"]) != 64 or any(x not in "0123456789abcdef" for x in c["value"]):
            raise ValueError("invalid checksum")
    if value["summary"] != {"file_count": len(keys), "total_size": sum(f["size"] for f in value["files"])}:
        raise ValueError("invalid summary")
