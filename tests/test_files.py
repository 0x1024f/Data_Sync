import json

from data_sync.files import FileCollector
from data_sync.state import State


def ready_batch(config, state, data=b"data"):
    (config.files[0].root / "batch1_primary.dat").write_bytes(data)
    collector = FileCollector(config, state)
    for now in (10, 15, 25):
        collector.scan(config.files[0], now)
    return state.one("SELECT * FROM batches WHERE status='READY'")


def test_stability_quiet_and_late(config, state):
    source = config.files[0]
    first = source.root / "batch1_primary.dat"
    first.write_bytes(b"start")
    collector = FileCollector(config, state)
    collector.scan(source, 10)
    first.write_bytes(b"longer")
    collector.scan(source, 14)
    collector.scan(source, 18)
    assert state.one("SELECT status FROM files")[0] == "STABILIZING"
    collector.scan(source, 19)
    (source.root / "batch1_secondary.dat").write_bytes(b"second")
    collector.scan(source, 28)
    collector.scan(source, 33)
    collector.scan(source, 42)
    assert state.one("SELECT status FROM batches")[0] == "OPEN"
    collector.scan(source, 43)
    batch = state.one("SELECT * FROM batches")
    assert batch["status"] == "READY"
    manifest = json.loads(batch["manifest"])
    assert len(manifest["files"]) == 2
    (source.root / "batch1_late.dat").write_bytes(b"late")
    collector.scan(source, 50)
    assert state.one("SELECT error FROM files WHERE path LIKE '%late%'")[0] == "LATE_FILE"
    assert state.one("SELECT manifest FROM batches")[0] == batch["manifest"]


def test_new_only_persists_across_restart(config, state):
    source = config.files[0]
    source.initial_scan = "new_only"
    (source.root / "batch1_primary.dat").write_bytes(b"old")
    FileCollector(config, state).scan(source, 1)
    (source.root / "batch2_primary.dat").write_bytes(b"while stopped")
    other = State(config.agent.work_dir / "state.sqlite3")
    try:
        other.configure(config, 2)
        for now in (3, 8, 18):
            FileCollector(config, other).scan(source, now)
        assert other.one("SELECT status FROM files WHERE path LIKE 'batch1%'")[0] == "IGNORED"
        assert other.one("SELECT batch_no FROM batches")[0] == "batch2"
    finally:
        other.close()


def test_missing_file_prevents_sealing(config, state):
    source = config.files[0]
    path = source.root / "batch1_primary.dat"
    path.write_bytes(b"x")
    collector = FileCollector(config, state)
    collector.scan(source, 1)
    collector.scan(source, 6)
    path.unlink()
    collector.scan(source, 20)
    assert state.one("SELECT status FROM batches")[0] == "OPEN"


def test_new_target_only_new_batches(config, state):
    ready_batch(config, state)
    new = config.targets[0].model_copy(update={"id": "b", "host": "b.example.internal"})
    config.targets.append(new)
    state.configure(config, 30)
    assert state.one("SELECT count(*) FROM tasks")[0] == 1
    (config.files[0].root / "batch2_primary.dat").write_bytes(b"new")
    for now in (31, 36, 46):
        FileCollector(config, state).scan(config.files[0], now)
    assert state.one("SELECT count(*) FROM tasks WHERE target='b'")[0] == 1


def test_local_snapshot_survives_original_mutation(config, state):
    batch = ready_batch(config, state, b"original")
    from pathlib import Path
    path = next(iter(json.loads(batch["local_files"]).values()))
    (config.files[0].root / "batch1_primary.dat").write_bytes(b"replacement")
    FileCollector(config, state).scan(config.files[0], 30)
    assert Path(path).read_bytes() == b"original"
    assert state.one("SELECT error FROM files")[0] == "SOURCE_CHANGED_AFTER_SEAL"
