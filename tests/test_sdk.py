import base64
import hashlib
import json
from pathlib import Path

import boto3
from botocore.stub import Stubber

from data_sync.manifest import canonical
from data_sync.transport import Delivery
from tests.test_mysql import ready_mysql as ready_batch
from tests.test_transport import claim


def test_real_sdk_conditional_request_shapes(config, state):
    batch = ready_batch(config, state)
    manifest = json.loads(batch["manifest"])
    file = manifest["files"][0]
    data = Path(json.loads(batch["local_files"])[file["target_key"]]).read_bytes()
    client = boto3.client("s3", region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test",
                          endpoint_url="https://example.invalid")
    stub = Stubber(client)
    bucket = config.targets[0].bucket
    def head(key, body):
        stub.add_response("head_object", {"ContentLength": len(body), "Metadata": {"sha256": hashlib.sha256(body).hexdigest()}}, {"Bucket": bucket, "Key": key})
    def missing(key):
        stub.add_client_error("head_object", service_error_code="404", http_status_code=404,
                              expected_params={"Bucket": bucket, "Key": key})
    def put(key, body, content_type):
        stub.add_response("put_object", {}, {"Bucket": bucket, "Key": key, "Body": body,
            "Metadata": {"sha256": hashlib.sha256(body).hexdigest()}, "ContentType": content_type,
            "IfNoneMatch": "*", "ContentMD5": base64.b64encode(hashlib.md5(body).digest()).decode()})
    missing(file["target_key"])
    missing(file["target_key"])
    put(file["target_key"], data, file["content_type"])
    head(file["target_key"], data)
    head(file["target_key"], data)
    payload = canonical(manifest)
    missing(batch["manifest_key"])
    put(batch["manifest_key"], payload, "application/json")
    head(batch["manifest_key"], payload)
    with stub:
        Delivery(state, config.targets[0], client).run(claim(state))
        stub.assert_no_pending_responses()
    assert state.one("SELECT status FROM tasks")[0] == "COMMITTED"
    assert "IfNoneMatch" in client.meta.service_model.operation_model("CompleteMultipartUpload").input_shape.members


def test_real_sdk_file_overwrite_shape(config, state):
    from tests.test_files import ready_batch as ready_file
    ready_file(config, state)
    client = boto3.client("s3", region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test",
                          endpoint_url="https://example.invalid")
    stub = Stubber(client)
    key, body, bucket = "batch1_primary.dat", b"data", config.targets[0].bucket
    params = {"Bucket": bucket, "Key": key}
    for _ in range(2):
        stub.add_response("head_object", {"ContentLength": 3, "Metadata": {"sha256": "old"}}, params)
    stub.add_response("put_object", {}, dict(params, Body=body,
        Metadata={"sha256": hashlib.sha256(body).hexdigest()}, ContentType="application/octet-stream",
        ContentMD5=base64.b64encode(hashlib.md5(body).digest()).decode()))
    for _ in range(2):
        stub.add_response("head_object", {"ContentLength": 4, "Metadata": {"sha256": hashlib.sha256(body).hexdigest()}}, params)
    with stub:
        Delivery(state, config.targets[0], client).run(claim(state))
        stub.assert_no_pending_responses()
    assert state.one("SELECT status FROM tasks")[0] == "COMMITTED"
