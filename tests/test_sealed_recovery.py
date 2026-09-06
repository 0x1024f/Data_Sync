import pytest

from data_sync.files import FileCollector


def test_sealed_inventory_is_not_rewritten_on_restart(config, state, monkeypatch):
    source = config.files[0]
    path = source.root / "batch1_primary.dat"
    path.write_bytes(b"original")
    collector = FileCollector(config, state)
    collector.scan(source, 10)
    collector.scan(source, 15)
    original = collector._snapshot
    def crash(*args):
        raise OSError("restart after sealing")
    monkeypatch.setattr(collector, "_snapshot", crash)
    with pytest.raises(OSError):
        collector.scan(source, 25)
    path.write_bytes(b"modified after crash")
    monkeypatch.setattr(collector, "_snapshot", original)
    collector.scan(source, 30)
    assert state.one("SELECT size FROM files")[0] == len(b"original")
    assert state.one("SELECT status FROM batches")[0] == "QUARANTINED"
    assert state.claim("sz", "worker") is None
