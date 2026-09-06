import hashlib


class S3Error(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}


class FakeS3:
    """Conditional object store with remote multipart state and failure hooks."""
    def __init__(self):
        self.objects = {}
        self.uploads = {}
        self.events = []
        self.failed = False
        self.fail_part = None
        self.fail_manifest = False
        self.drop_complete_response = False
        self.upload_number = 0

    def head_object(self, Bucket, Key):
        if self.failed:
            raise S3Error("ServiceUnavailable")
        if Key not in self.objects:
            raise S3Error("404")
        body, metadata = self.objects[Key]
        return {"ContentLength": len(body), "Metadata": metadata}

    def put_object(self, Bucket, Key, Body, Metadata, **kwargs):
        assert kwargs["IfNoneMatch"] == "*"
        if self.fail_manifest and Key.endswith("manifest.json"):
            raise S3Error("ServiceUnavailable")
        if Key in self.objects:
            raise S3Error("PreconditionFailed")
        self.objects[Key] = (Body, Metadata)
        self.events.append(("put", Key))

    def create_multipart_upload(self, Bucket, Key, Metadata, **kwargs):
        self.upload_number += 1
        upload_id = str(self.upload_number)
        self.uploads[upload_id] = {"key": Key, "metadata": Metadata, "parts": {}}
        return {"UploadId": upload_id}

    def list_parts(self, UploadId, **kwargs):
        if UploadId not in self.uploads:
            raise S3Error("NoSuchUpload")
        parts = self.uploads[UploadId]["parts"]
        return {"Parts": [{"PartNumber": n, "Size": len(b), "ETag": hashlib.md5(b).hexdigest()} for n, b in parts.items()], "IsTruncated": False}

    def upload_part(self, UploadId, PartNumber, Body, **kwargs):
        if PartNumber == self.fail_part:
            raise S3Error("ServiceUnavailable")
        self.uploads[UploadId]["parts"][PartNumber] = Body
        self.events.append(("part", PartNumber))
        return {"ETag": hashlib.md5(Body).hexdigest()}

    def complete_multipart_upload(self, Key, UploadId, MultipartUpload, **kwargs):
        assert kwargs["IfNoneMatch"] == "*"
        if Key in self.objects:
            raise S3Error("PreconditionFailed")
        upload = self.uploads.pop(UploadId)
        self.objects[Key] = (b"".join(upload["parts"][p["PartNumber"]] for p in MultipartUpload["Parts"]), upload["metadata"])
        self.events.append(("complete", Key))
        if self.drop_complete_response:
            self.drop_complete_response = False
            raise S3Error("ServiceUnavailable")

    def abort_multipart_upload(self, UploadId, **kwargs):
        if self.uploads.pop(UploadId, None) is None:
            raise S3Error("NoSuchUpload")


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def execute(self, query, args=None):
        self.connection.queries.append((query, args))
        if "information_schema.COLUMNS" in query:
            self.rows = [{"COLUMN_NAME": "id", "DATA_TYPE": "bigint", "EXTRA": "auto_increment"}, {"COLUMN_NAME": "value", "DATA_TYPE": "varchar", "EXTRA": ""}]
        elif "information_schema.STATISTICS" in query:
            self.rows = [{"COLUMN_NAME": "id"}]
        elif "MAX(" in query:
            self.rows = [{"high": max([r["id"] for r in self.connection.rows], default=0)}]
        else:
            self.rows = sorted([r for r in self.connection.rows if r["id"] > args[0]], key=lambda r: r["id"])[:args[1]]

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0]


class FakeMySQL:
    def __init__(self, rows):
        self.rows, self.queries = rows, []

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        pass
