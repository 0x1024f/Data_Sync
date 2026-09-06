import pytest

from data_sync.config import Config
from data_sync.state import State


@pytest.fixture
def config(tmp_path):
    root = tmp_path / "input"
    root.mkdir()
    return Config.model_validate({
        "agent": {"source_id": "station", "work_dir": tmp_path / "work", "min_free_bytes": 0},
        "files": [{"id": "sat", "system": "satellite", "root": root, "initial_scan": "existing_and_new",
                   "stable_seconds": 5, "quiet_seconds": 10,
                   "filename_regex": r"(?P<batch_no>batch\d+)_(?P<role>\w+)\.dat"}],
        "targets": [{"id": "sz", "host": "sz.example.internal", "bucket": "test-bucket",
                     "access_key": "env:TEST_KEY", "secret_key": "env:TEST_SECRET"}]})


@pytest.fixture
def state(config):
    value = State(config.agent.work_dir / "state.sqlite3")
    value.configure(config, now=0)
    yield value
    value.close()
