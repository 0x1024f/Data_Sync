import copy
import json

import pytest
from pydantic import ValidationError

from data_sync.config import Config, MySQLSource, safe_key
from data_sync.manifest import identity, validate_manifest
from data_sync.runtime import ProcessLock
from tests.test_files import ready_batch


def test_manifest_identity_and_validation(config, state):
    manifest = json.loads(ready_batch(config, state)["manifest"])
    validate_manifest(manifest)
    other = copy.deepcopy(manifest)
    other["created_at"] = "2030-01-01T00:00:00+00:00"
    assert identity(other) == manifest["manifest_id"]
    other["files"][0]["size"] += 1
    with pytest.raises(ValueError):
        validate_manifest(other)


@pytest.mark.parametrize("key", ["../x", "a/../b", "/absolute", "a\\b", "a//b", "a/\x00b"])
def test_unsafe_keys(key):
    with pytest.raises(ValueError):
        safe_key(key)


def test_strict_config(config):
    data = config.model_dump()
    data["targets"][0]["secret_key"] = "literal-secret"
    with pytest.raises(ValidationError):
        Config.model_validate(data)
    data = config.model_dump()
    data["agent"]["unknown"] = True
    with pytest.raises(ValidationError):
        Config.model_validate(data)


def test_process_lock(tmp_path):
    with ProcessLock(tmp_path / "lock"):
        with pytest.raises(RuntimeError):
            with ProcessLock(tmp_path / "lock"):
                pass
