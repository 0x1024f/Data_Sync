import json
from pathlib import Path

import pytest

from data_sync.files import FileCollector
from data_sync.mysql import MySQLCollector
from data_sync.transport import Delivery
from tests.fakes import FakeMySQL, FakeS3
from tests.test_files import ready_batch
from tests.test_mysql import source
from tests.test_transport import claim


def test_disk_full_does_not_publish(config, state, monkeypatch):
    from collections import namedtuple
    import data_sync.files
    usage = namedtuple("usage", "total used free")
    monkeypatch.setattr(data_sync.files.shutil, "disk_usage", lambda _: usage(10, 10, 0))
    (config.files[0].root / "batch1_primary.dat").write_bytes(b"x")
    collector = FileCollector(config, state)
    collector.scan(config.files[0], 10)
    collector.scan(config.files[0], 15)
    with pytest.raises(OSError):
        collector.scan(config.files[0], 25)
    assert state.one("SELECT status FROM batches")[0] == "SEALED"
    assert state.claim("sz", "worker") is None


def test_snapshot_tamper_prevents_manifest(config, state):
    batch = ready_batch(config, state)
    path = Path(next(iter(json.loads(batch["local_files"]).values())))
    path.write_bytes(b"evil")
    client = FakeS3()
    Delivery(state, config.targets[0], client).run(claim(state))
    assert not client.objects
    assert state.one("SELECT status FROM tasks")[0] == "BLOCKED"


def test_future_target_and_reenable(config, state):
    from datetime import datetime, timezone
    future = config.targets[0].model_copy(update={"id": "future", "host": "future.example.internal", "enabled_at": datetime.fromtimestamp(100, timezone.utc)})
    config.targets.append(future)
    state.configure(config, 1)
    ready_batch(config, state)
    assert state.one("SELECT count(*) FROM tasks WHERE target='future'")[0] == 0
    (config.files[0].root / "batch2_primary.dat").write_bytes(b"next")
    FileCollector(config, state).scan(config.files[0], 101)
    assert state.one("SELECT count(*) FROM tasks WHERE target='future'")[0] == 1
    future.enabled = False
    state.configure(config, 110)
    (config.files[0].root / "batch3_primary.dat").write_bytes(b"disabled")
    FileCollector(config, state).scan(config.files[0], 111)
    future.enabled = True
    state.configure(config, 120)
    assert state.one("SELECT count(*) FROM tasks WHERE target='future'")[0] == 1


def test_endpoint_reuse_rejected(config, state):
    config.targets[0].host = "other.example.internal"
    with pytest.raises(ValueError):
        state.configure(config, 30)


def test_envelope_recovers_before_any_query(config, state, monkeypatch):
    src = source(config, state)
    db = FakeMySQL([{"id": 1, "value": "x"}])
    collector = MySQLCollector(config, state, lambda _: db)
    original = collector.recover_prepared
    def fail(_):
        envelope = config.agent.work_dir / "prepared" / (src.id + ".json")
        if envelope.exists():
            raise OSError("crash before registering prepared batch")
    monkeypatch.setattr(collector, "recover_prepared", fail)
    with pytest.raises(OSError):
        collector.poll(src, 10)
    assert state.one("SELECT count(*) FROM batches")[0] == 0
    assert state.one("SELECT cursor FROM checkpoints")[0] == 0
    monkeypatch.setattr(collector, "recover_prepared", original)
    collector.recover_prepared(src)
    assert state.one("SELECT cursor FROM checkpoints")[0] == 1
    assert state.one("SELECT status FROM batches")[0] == "READY"


def test_scan_permission_failure_cannot_seal(config, state, monkeypatch):
    import data_sync.files
    root = config.files[0].root
    (root / "batch1_primary.dat").write_bytes(b"x")
    collector = FileCollector(config, state)
    collector.scan(config.files[0], 10)
    collector.scan(config.files[0], 15)
    def broken_walk(*args, **kwargs):
        kwargs["onerror"](PermissionError("denied"))
        yield
    monkeypatch.setattr(data_sync.files.os, "walk", broken_walk)
    with pytest.raises(PermissionError):
        collector.scan(config.files[0], 30)
    assert state.one("SELECT status FROM batches")[0] == "OPEN"
