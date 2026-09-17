"""S3 conditional writes, durable multipart resume and per-target delivery."""
import base64
import hashlib
import json
import logging
import math
import random
import time
from collections.abc import Mapping
from pathlib import Path

from .config import secret
from .manifest import canonical, digest_file, validate_manifest

log = logging.getLogger(__name__)


class Conflict(Exception):
    pass


def error_code(error):
    fallback = type(error).__name__
    response = getattr(error, "response", None)
    if not isinstance(response, Mapping):
        return fallback
    detail = response.get("Error")
    if not isinstance(detail, Mapping):
        return fallback
    code = detail.get("Code")
    return str(code) if code is not None else fallback


def client_for(target):
    import boto3
    from botocore.config import Config
    return boto3.client("s3", endpoint_url=f"https://{target.host}:{target.port}", region_name=target.region,
                        aws_access_key_id=secret(target.access_key), aws_secret_access_key=secret(target.secret_key),
                        verify=str(target.ca_bundle) if target.ca_bundle else True,
                        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                                      proxies={},
                                      connect_timeout=target.timeout_seconds, read_timeout=target.timeout_seconds,
                                      retries={"mode": "standard", "total_max_attempts": 2}))


class Delivery:
    def __init__(self, state, target, client=None, stop=None):
        self.state, self.target = state, target
        self.client = client if client is not None else client_for(target)
        self.stop = stop

    def _head(self, key):
        try:
            return self.client.head_object(Bucket=self.target.bucket, Key=key)
        except Exception as error:
            if error_code(error) in ("404", "NoSuchKey", "NotFound"):
                return None
            raise

    def _check(self, key, size, digest):
        head = self._head(key)
        if head is None:
            return False
        if head["ContentLength"] != size or head.get("Metadata", {}).get("sha256") != digest:
            raise Conflict("object key already holds different content")
        return True

    def run(self, task):
        try:
            batch = self.state.one("SELECT * FROM batches WHERE id=?", (task["batch"],))
            manifest = json.loads(batch["manifest"])
            validate_manifest(manifest)
            local = json.loads(batch["local_files"])
            self.state.phase(task, "UPLOADING_FILES")
            for f in manifest["files"]:
                if self.stop and self.stop.is_set():
                    raise InterruptedError("stopping")
                self._file(task, f, Path(local[f["target_key"]]))
            self.state.phase(task, "VERIFYING")
            for f in manifest["files"]:
                if not self._check(f["target_key"], f["size"], f["checksum"]["value"]):
                    raise OSError("object disappeared before manifest commit")
            self.state.phase(task, "UPLOADING_MANIFEST")
            payload = canonical(manifest)
            self._put(batch["manifest_key"], payload, hashlib.sha256(payload).hexdigest(), "application/json")
            self.state.finish(task, "COMMITTED")
            log.info("task_committed", extra={"target": self.target.id, "task": task["id"]})
        except Exception as error:
            code = error_code(error)
            permanent = isinstance(error, (Conflict, ValueError)) or code in (
                "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "NoSuchBucket", "InvalidRequest", "NotImplemented", "SSLError")
            delay = min(self.target.retry_max_seconds, self.target.retry_initial_seconds * 2 ** min(task["attempts"], 20))
            self.state.finish(task, "BLOCKED" if permanent else "RETRY_WAIT", code, random.uniform(delay / 2, delay))
            # Never log raw SDK/SQL error messages: they may contain secrets or rows.
            log.error("task_failed", extra={"target": self.target.id, "task": task["id"], "code": code})

    def _put(self, key, body, digest, content_type):
        if hashlib.sha256(body).hexdigest() != digest:
            raise Conflict("local payload changed before upload")
        if self._check(key, len(body), digest):
            return
        try:
            self.client.put_object(Bucket=self.target.bucket, Key=key, Body=body,
                                   Metadata={"sha256": digest}, ContentType=content_type, IfNoneMatch="*",
                                   ContentMD5=base64.b64encode(hashlib.md5(body).digest()).decode("ascii"))
        except Exception as error:
            if error_code(error) not in ("PreconditionFailed", "412", "ConditionalRequestConflict", "409"):
                raise
            if not self._check(key, len(body), digest):
                raise
        if not self._check(key, len(body), digest):
            raise OSError("object verification failed")

    def _file(self, task, f, path):
        key, digest, size = f["target_key"], f["checksum"]["value"], f["size"]
        if self._check(key, size, digest):
            self._discard_upload(task, key)
            return
        last_refresh = [0.0]
        def heartbeat():
            if self.stop and self.stop.is_set():
                raise InterruptedError("stopping")
            now = time.monotonic()
            if now - last_refresh[0] > 30:
                self.state.phase(task, "UPLOADING_FILES")
                last_refresh[0] = now
        actual_size, actual_digest = digest_file(path, heartbeat)
        if (actual_size, actual_digest) != (size, digest):
            raise Conflict("local snapshot content changed")
        if size < self.target.multipart_threshold:
            self._put(key, path.read_bytes(), digest, f["content_type"])
            return
        part_size = max(self.target.part_size, math.ceil(size / 10000))
        row = self.state.one("SELECT * FROM uploads WHERE task=? AND key=?", (task["id"], key))
        if row:
            upload_id = row["upload_id"]
            if row["part_size"]:
                part_size = row["part_size"]
            else:
                # Legacy uploads did not persist geometry; restart that multipart
                # session rather than concatenate parts made with another size.
                self._discard_upload(task, key)
                return self._file(task, f, path)
        else:
            upload_id = self.client.create_multipart_upload(Bucket=self.target.bucket, Key=key, Metadata={"sha256": digest}, ContentType=f["content_type"])["UploadId"]
            self.state.db.execute("INSERT INTO uploads(task,key,upload_id,part_size) VALUES (?,?,?,?)", (task["id"], key, upload_id, part_size))
        parts = {}
        try:
            # Server listing reconciles successful parts whose response or local
            # checkpoint was lost. Pagination is required for large uploads.
            marker = 0
            while True:
                response = self.client.list_parts(Bucket=self.target.bucket, Key=key, UploadId=upload_id, PartNumberMarker=marker)
                for part in response.get("Parts", []):
                    parts[part["PartNumber"]] = part
                if not response.get("IsTruncated"):
                    break
                marker = response["NextPartNumberMarker"]
            completed = []
            stream_digest = hashlib.sha256()
            with path.open("rb") as handle:
                number = 0
                for body in iter(lambda: handle.read(part_size), b""):
                    stream_digest.update(body)
                    number += 1
                    if self.stop and self.stop.is_set():
                        raise InterruptedError("stopping")
                    self.state.phase(task, "UPLOADING_FILES")
                    part = parts.get(number)
                    if part is None or part["Size"] != len(body):
                        result = self.client.upload_part(Bucket=self.target.bucket, Key=key, UploadId=upload_id,
                                                         PartNumber=number, Body=body,
                                                         ContentMD5=base64.b64encode(hashlib.md5(body).digest()).decode("ascii"))
                        part = {"PartNumber": number, "ETag": result["ETag"], "Size": len(body)}
                        parts[number] = part
                        self.state.db.execute("UPDATE uploads SET parts=? WHERE task=? AND key=?", (canonical(parts).decode(), task["id"], key))
                    completed.append({"PartNumber": number, "ETag": part["ETag"]})
            if stream_digest.hexdigest() != digest:
                self._discard_upload(task, key)
                raise Conflict("snapshot changed during multipart upload")
            self.client.complete_multipart_upload(Bucket=self.target.bucket, Key=key, UploadId=upload_id,
                                                  MultipartUpload={"Parts": completed}, IfNoneMatch="*")
        except Exception as error:
            code = error_code(error)
            if code == "NoSuchUpload":
                self.state.db.execute("DELETE FROM uploads WHERE task=? AND key=?", (task["id"], key))
                if self._check(key, size, digest):
                    return
            elif code in ("PreconditionFailed", "412", "ConditionalRequestConflict", "409"):
                if self._check(key, size, digest):
                    self._discard_upload(task, key)
                    return
                self._discard_upload(task, key)
            raise
        if not self._check(key, size, digest):
            raise OSError("multipart verification failed")
        self.state.db.execute("DELETE FROM uploads WHERE task=? AND key=?", (task["id"], key))

    def _discard_upload(self, task, key):
        row = self.state.one("SELECT upload_id FROM uploads WHERE task=? AND key=?", (task["id"], key))
        if row:
            try:
                self.client.abort_multipart_upload(Bucket=self.target.bucket, Key=key, UploadId=row[0])
            except Exception as error:
                if error_code(error) != "NoSuchUpload":
                    raise
            self.state.db.execute("DELETE FROM uploads WHERE task=? AND key=?", (task["id"], key))
