import json

from data_sync.runtime import Agent
from data_sync.state import State
from tests.fakes import FakeS3


def test_agent_once_end_to_end(config, monkeypatch):
    import data_sync.transport
    config.files[0].stable_seconds = 0
    config.files[0].quiet_seconds = 0
    (config.files[0].root / "batch1_primary.dat").write_bytes(b"end-to-end")
    client = FakeS3()
    monkeypatch.setattr(data_sync.transport, "client_for", lambda _: client)
    Agent(config).run(once=True)
    state = State(config.agent.work_dir / "state.sqlite3")
    try:
        assert state.one("SELECT status FROM tasks")[0] == "COMMITTED"
        manifest_key = state.one("SELECT manifest_key FROM batches")[0]
        manifest = json.loads(client.objects[manifest_key][0])
        assert client.objects[manifest["files"][0]["target_key"]][0] == b"end-to-end"
        assert client.events[-1] == ("put", manifest_key)
    finally:
        state.close()


def test_readonly_status_and_cli_validate(config, tmp_path, monkeypatch, capsys):
    import yaml
    from data_sync.cli import main
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(json.loads(config.model_dump_json())), encoding="utf-8")
    monkeypatch.setenv("TEST_KEY", "key")
    monkeypatch.setenv("TEST_SECRET", "never-display-this")
    assert main(["--config", str(path), "validate"]) == 0
    assert "never-display-this" not in capsys.readouterr().out
    state = State(config.agent.work_dir / "state.sqlite3")
    state.configure(config)
    state.close()
    assert main(["--config", str(path), "status"]) == 0
    assert json.loads(capsys.readouterr().out)["tasks"] == []
