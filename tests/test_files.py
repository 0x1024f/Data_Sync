import json
from pathlib import Path

from data_sync.files import FileCollector
from data_sync.state import State


def ready_batch(config, state, data=b"data"):
    (config.files[0].root / "batch1_primary.dat").write_bytes(data)
    for now in (10, 15):
        FileCollector(config, state).scan(config.files[0], now)
    return state.one("SELECT * FROM batches WHERE status='READY'")


def test_late_file_is_independent(config, state):
    first = ready_batch(config, state)
    (config.files[0].root / "batch1_secondary.dat").write_bytes(b"second")
    for now in (20, 25):
        FileCollector(config, state).scan(config.files[0], now)
    assert state.one("SELECT count(*) FROM batches")[0] == 2
    assert state.one("SELECT manifest FROM batches WHERE id=?", (first["id"],))[0] == first["manifest"]


def test_new_only_persists_across_restart(config, state):
    source = config.files[0]
    source.initial_scan = "new_only"
    (source.root / "old").write_bytes(b"old")
    FileCollector(config, state).scan(source, 1)
    (source.root / "new").write_bytes(b"while stopped")
    other = State(config.agent.work_dir / "state.sqlite3")
    try:
        other.configure(config, 2)
        for now in (3, 8):
            FileCollector(config, other).scan(source, now)
        assert other.one("SELECT status FROM files WHERE path='old'")[0] == "IGNORED"
        assert other.one("SELECT object_key FROM batches")[0] == "new"
    finally:
        other.close()


def test_missing_file_does_not_publish(config, state):
    path = config.files[0].root / "a"
    path.write_bytes(b"x")
    collector = FileCollector(config, state)
    collector.scan(config.files[0], 1)
    path.unlink()
    collector.scan(config.files[0], 6)
    assert not state.all("SELECT * FROM batches")
    assert state.one("SELECT error FROM files")[0] == "SOURCE_MISSING"


def test_new_target_only_new_versions(config, state):
    ready_batch(config, state)
    config.targets.append(config.targets[0].model_copy(update={"id": "b", "host": "b.example.internal"}))
    state.configure(config, 30)
    assert state.one("SELECT count(*) FROM tasks")[0] == 1
    (config.files[0].root / "new").write_bytes(b"new")
    for now in (31, 36):
        FileCollector(config, state).scan(config.files[0], now)
    assert state.one("SELECT count(*) FROM tasks WHERE target='b'")[0] == 1


def test_local_snapshot_survives_original_mutation(config, state):
    batch = ready_batch(config, state, b"original")
    snapshot = Path(next(iter(json.loads(batch["local_files"]).values())))
    (config.files[0].root / "batch1_primary.dat").write_bytes(b"replacement")
    for now in (30, 35):
        FileCollector(config, state).scan(config.files[0], now)
    assert snapshot.read_bytes() == b"original"
    assert state.one("SELECT count(*) FROM batches")[0] == 2
