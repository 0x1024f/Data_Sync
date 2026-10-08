"""S3 条件写入、持久化分片断点续传及各目标端的独立投递。"""
import base64
import hashlib
import json
import logging
import math
import random
import time
from collections.abc import Mapping
from pathlib import Path

from .config import safe_key, secret
from .manifest import canonical, digest_file, validate_manifest
from .notify import NotificationError, post_notification

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
    return boto3.client("s3", endpoint_url=f"{target.scheme}://{target.host}:{target.port}", region_name=target.region,
                        aws_access_key_id=secret(target.access_key), aws_secret_access_key=secret(target.secret_key),
                        verify=str(target.ca_bundle) if target.ca_bundle else True,
                        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                                      proxies={},
                                      connect_timeout=target.timeout_seconds, read_timeout=target.timeout_seconds,
                                      retries={"mode": "standard", "total_max_attempts": 2}))


class Delivery:
    def __init__(self, state, target, client=None, stop=None, notifier=None):
        self.state, self.target = state, target
        self._client = client
        self.stop = stop
        self.notifier = notifier if notifier is not None else post_notification

    @property
    def client(self):
        if self._client is None:
            self._client = client_for(self.target)
        return self._client

    def _head(self, key):
        try:
            return self.client.head_object(Bucket=self.target.bucket, Key=key)
        except Exception as error:
            if error_code(error) in ("404", "NoSuchKey", "NotFound"):
                return None
            raise

    def _check(self, key, size, digest, overwrite=False):
        head = self._head(key)
        if head is None:
            return False
        if head["ContentLength"] != size or head.get("Metadata", {}).get("sha256") != digest:
            if overwrite:
                return False
            raise Conflict("object key already holds different content")
        return True

    def run(self, task):
        try:
            if self.state.one("SELECT 1 FROM notifications WHERE task=?", (task["id"],)):
                self._notify(task)
                return
            batch = self.state.one("SELECT * FROM batches WHERE id=?", (task["batch"],))
            manifest = json.loads(batch["manifest"])
            overwrite = batch["kind"] == "file"
            if overwrite:
                if len(manifest["files"]) != 1:
                    raise ValueError("file task must contain exactly one file")
                f = manifest["files"][0]
                if safe_key(f["target_key"]) != batch["object_key"] or type(f["size"]) is not int or f["size"] < 0:
                    raise ValueError("invalid file descriptor")
                checksum = f["checksum"]
                if checksum["algorithm"] != "sha256" or len(checksum["value"]) != 64 or any(c not in "0123456789abcdef" for c in checksum["value"]):
                    raise ValueError("invalid checksum")
            else:
                validate_manifest(manifest)
            keys = [f["target_key"] for f in manifest["files"]]
            if not overwrite:
                keys.append(batch["manifest_key"])
            self.state.reserve_keys(task, batch, keys)
            local = json.loads(batch["local_files"])
            self.state.phase(task, "UPLOADING_FILES")
            for f in manifest["files"]:
                if self.stop and self.stop.is_set():
                    raise InterruptedError("stopping")
                self._file(task, f, Path(local[f["target_key"]]), overwrite=overwrite)
            self.state.phase(task, "VERIFYING")
            for f in manifest["files"]:
                if not self._check(f["target_key"], f["size"], f["checksum"]["value"]):
                    raise OSError("object disappeared before commit")
            if not overwrite:
                self.state.phase(task, "UPLOADING_MANIFEST")
                payload = canonical(manifest)
                self._put(batch["manifest_key"], payload, hashlib.sha256(payload).hexdigest(), "application/json")
            if overwrite and self.target.notify is not None:
                config = self.target.notify
                key = manifest["files"][0]["target_key"]
                path = config.path_prefix.rstrip("/") + "/" + key if config.path_prefix else key
                self.state.prepare_notification(task, config, {
                    "moduleType": config.module_type, "bucketName": self.target.bucket,
                    "filePathList": [path],
                })
                self._notify(task)
                return
            self.state.finish(task, "COMMITTED")
            log.info("task_committed", extra={"target": self.target.id, "task": task["id"]})
        except Exception as error:
            code = error_code(error)
            permanent = isinstance(error, (Conflict, ValueError)) or code in (
                "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "NoSuchBucket", "InvalidRequest", "NotImplemented", "SSLError")
            delay = min(self.target.retry_max_seconds, self.target.retry_initial_seconds * 2 ** min(task["attempts"], 20))
            self.state.finish(task, "BLOCKED" if permanent else "RETRY_WAIT", code, random.uniform(delay / 2, delay))
            # 禁止记录原始 SDK/SQL 错误信息，其中可能包含敏感信息或行数据。
            log.error("task_failed", extra={"target": self.target.id, "task": task["id"], "code": code})

    def _notify(self, task):
        row = self.state.one("SELECT * FROM notifications WHERE task=?", (task["id"],))
        extra = {"target": self.target.id, "task": task["id"], "notification_attempt": row["attempts"]}
        if row["status"] in ("SUCCEEDED", "FAILED"):
            self.state.finish(task, "COMMITTED")
            return
        if row["attempts"] >= 3:
            self.state.finish_notification(task, row["error"] or "Interrupted")
            log.error("notification_exhausted", extra={**extra, "code": row["error"] or "Interrupted"})
            return
        if self.stop and self.stop.is_set():
            self.state.finish(task, "NOTIFY_RETRY_WAIT", delay=max(0, row["next_retry"] - time.time()))
            return
        extra["notification_attempt"] = self.state.start_notification(task)
        error = None
        try:
            self.notifier(row["url"], json.loads(row["payload"]), row["timeout_seconds"])
        except Exception as exc:
            error = str(exc) if isinstance(exc, NotificationError) else type(exc).__name__
        status = self.state.finish_notification(task, error)
        if error:
            log.error("notification_exhausted" if status == "FAILED" else "notification_failed",
                      extra={**extra, "code": error})
        else:
            log.info("notification_succeeded", extra=extra)
        if status != "RETRY_WAIT":
            log.info("task_committed", extra=extra)

    def _put(self, key, body, digest, content_type, overwrite=False):
        if hashlib.sha256(body).hexdigest() != digest:
            raise Conflict("local payload changed before upload")
        if self._check(key, len(body), digest, overwrite=overwrite):
            return
        try:
            self.client.put_object(Bucket=self.target.bucket, Key=key, Body=body,
                                   Metadata={"sha256": digest}, ContentType=content_type,
                                   **({} if overwrite else {"IfNoneMatch": "*"}),
                                   ContentMD5=base64.b64encode(hashlib.md5(body).digest()).decode("ascii"))
        except Exception as error:
            if error_code(error) not in ("PreconditionFailed", "412", "ConditionalRequestConflict", "409"):
                raise
            if not self._check(key, len(body), digest):
                raise
        if not self._check(key, len(body), digest):
            raise OSError("object verification failed")

    def _file(self, task, f, path, overwrite=False):
        key, digest, size = f["target_key"], f["checksum"]["value"], f["size"]
        if self._check(key, size, digest, overwrite=overwrite):
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
            self._put(key, path.read_bytes(), digest, f["content_type"], overwrite=overwrite)
            return
        part_size = max(self.target.part_size, math.ceil(size / 10000))
        row = self.state.one("SELECT * FROM uploads WHERE task=? AND key=?", (task["id"], key))
        if row:
            upload_id = row["upload_id"]
            if row["part_size"]:
                part_size = row["part_size"]
            else:
                # 旧版上传未持久化分片布局信息；需重新开始该分片上传会话，
                # 避免拼接按不同大小生成的分片。
                self._discard_upload(task, key)
                return self._file(task, f, path, overwrite=overwrite)
        else:
            upload_id = self.client.create_multipart_upload(Bucket=self.target.bucket, Key=key, Metadata={"sha256": digest}, ContentType=f["content_type"])["UploadId"]
            self.state.db.execute("INSERT INTO uploads(task,key,upload_id,part_size) VALUES (?,?,?,?)", (task["id"], key, upload_id, part_size))
        parts = {}
        try:
            # 通过服务端分片列表核对已成功上传但响应或本地检查点丢失的分片。
            # 大型上传必须分页获取分片列表。
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
                                                  MultipartUpload={"Parts": completed},
                                                  **({} if overwrite else {"IfNoneMatch": "*"}))
        except Exception as error:
            code = error_code(error)
            if code == "NoSuchUpload":
                self.state.db.execute("DELETE FROM uploads WHERE task=? AND key=?", (task["id"], key))
                if self._check(key, size, digest, overwrite=overwrite):
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
