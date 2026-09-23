import pytest
from pydantic import ValidationError

from data_sync.config import Config, Target, load_config
from data_sync.transport import client_for
from pathlib import Path


def test_http_ip_and_sdk_endpoint(config, monkeypatch):
    data = config.targets[0].model_dump()
    data.update(scheme="http", host="192.168.0.21", port=30009)
    target = Target.model_validate(data)
    monkeypatch.setenv("TEST_KEY", "test-key")
    monkeypatch.setenv("TEST_SECRET", "test-secret")
    client = client_for(target)
    try:
        assert client.meta.endpoint_url == "http://192.168.0.21:30009"
        assert client.meta.config.signature_version == "s3v4"
        assert client.meta.config.s3["addressing_style"] == "path"
    finally:
        client.close()


@pytest.mark.parametrize("updates", [
    {"scheme": "http", "ca_bundle": "ca.pem"},
    {"scheme": "ftp"},
    {"scheme": "https", "host": "192.168.0.21"},
])
def test_invalid_transport_config(config, updates):
    data = config.targets[0].model_dump()
    data.update(updates)
    with pytest.raises(ValidationError):
        Target.model_validate(data)


@pytest.mark.parametrize("ca", [None, "ca.pem"])
def test_https_keeps_certificate_verification(config, monkeypatch, ca):
    data = config.targets[0].model_dump()
    data.pop("scheme")
    data["ca_bundle"] = ca
    target = Target.model_validate(data)
    monkeypatch.setenv("TEST_KEY", "test-key")
    monkeypatch.setenv("TEST_SECRET", "test-secret")
    captured = {}
    monkeypatch.setattr("boto3.client", lambda service, **kwargs: captured.update(kwargs))
    client_for(target)
    assert captured["endpoint_url"] == "https://sz.example.internal:443"
    assert captured["verify"] == (ca if ca else True)


def test_mixed_target_ports(config):
    data = config.model_dump()
    data["targets"].append(dict(data["targets"][0], id="http", scheme="http", host="192.168.0.21", port=30009))
    assert len(Config.model_validate(data).targets) == 2


def test_protocol_change_requires_new_target_id(config, state):
    state.configure(config, now=1)  # Existing HTTPS fingerprint remains compatible.
    config.targets[0].scheme = "http"
    with pytest.raises(ValueError):
        state.configure(config, now=2)


def test_http_example_loads():
    config = load_config(Path(__file__).resolve().parents[1] / "config.test.yaml")
    assert config.targets[0].scheme == "http"
    assert config.targets[0].ca_bundle is None
