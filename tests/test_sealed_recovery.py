import sqlite3

import pytest

from data_sync.files import FileCollector
from data_sync.state import LegacyFileStateError, State


def test_snapshot_before_registration_crash(config, state, monkeypatch):
    source = config.files[0]
    path = source.root / "a"
    path.write_bytes(b"original")
    collector = FileCollector(config, state)
    collector.scan(source, 10)
    original = state.register_file
    monkeypatch.setattr(state, "register_file", lambda *args: (_ for _ in ()).throw(OSError("crash")))
    collector.scan(source, 15)
    assert not state.all("SELECT * FROM tasks")
    path.write_bytes(b"modified after crash")
    monkeypatch.setattr(state, "register_file", original)
    collector.scan(source, 20)
    collector.scan(source, 25)
    assert state.one("SELECT count(*) FROM batches")[0] == 1
    assert state.claim("sz", "worker") is not None


def test_legacy_state_rejected_without_mutation(tmp_path):
    path = tmp_path / "old.sqlite3"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE sources (fingerprint TEXT)")
        db.execute('INSERT INTO sources VALUES (?)', ('{"root":"input"}',))
        db.execute("PRAGMA user_version=2")
    before = path.read_bytes()
    with pytest.raises(LegacyFileStateError, match="new work_dir"):
        State(path)
    assert path.read_bytes() == before
