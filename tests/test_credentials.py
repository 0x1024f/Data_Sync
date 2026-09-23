import json

import pytest
import yaml
from pydantic import ValidationError

from data_sync.cli import main
from data_sync.config import Target, secret
from data_sync.transport import client_for


def test_literal_credentials_reach_sdk_and_validate(config, monkeypatch, tmp_path, capsys):
    data = config.targets[0].model_dump()
    data.update(access_key="literal-key", secret_key="literal-password")
    config.targets[0] = Target.model_validate(data)
    monkeypatch.delenv("TEST_KEY", raising=False)
    monkeypatch.delenv("TEST_SECRET", raising=False)
    captured = {}
    monkeypatch.setattr("boto3.client", lambda service, **kwargs: captured.update(kwargs))
    client_for(config.targets[0])
    assert captured["aws_access_key_id"] == "literal-key"
    assert captured["aws_secret_access_key"] == "literal-password"
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(json.loads(config.model_dump_json())), encoding="utf-8")
    assert main(["--config", str(path), "validate"]) == 0
    output = capsys.readouterr()
    assert "literal-password" not in output.out + output.err + repr(config.targets[0])


def test_environment_credentials(monkeypatch):
    monkeypatch.setenv("TEST_SECRET", "resolved-password")
    assert secret("env:TEST_SECRET") == "resolved-password"
    monkeypatch.delenv("TEST_SECRET")
    with pytest.raises(ValueError, match="missing"):
        secret("env:TEST_SECRET")


@pytest.mark.parametrize("field", ["access_key", "secret_key"])
@pytest.mark.parametrize("value", ["", "  ", "env:", "env:INVALID-NAME"])
def test_invalid_credentials(config, field, value):
    data = config.targets[0].model_dump()
    data[field] = value
    with pytest.raises(ValidationError):
        Target.model_validate(data)
