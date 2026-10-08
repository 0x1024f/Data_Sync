import json

import pytest
import yaml
from pydantic import ValidationError

from data_sync.config import FileSource, load_config


def test_minimal_file_source():
    source = FileSource(id="files", system="local", root="input")
    assert source.initial_scan == "new_only"
    assert source.include == ["**/*"]
    assert source.recursive
    assert source.stable_seconds == 60


@pytest.mark.parametrize("field", ["filename_regex", "quiet_seconds", "path_layout",
                                  "obs_time_format", "timezone_offset", "prefix"])
def test_removed_options_rejected(field):
    with pytest.raises(ValidationError):
        FileSource.model_validate(dict(id="files", system="local", root="input", **{field: "old"}))


def test_paths_resolved_relative_to_config(config, tmp_path):
    data = json.loads(config.model_dump_json())
    data["files"][0]["root"] = "input"
    data["agent"]["work_dir"] = "new-work"
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    loaded = load_config(path)
    assert loaded.files[0].root == tmp_path / "input"
    assert loaded.agent.work_dir == tmp_path / "new-work"
