import json

import pytest

from data_sync.files import FileCollector
from data_sync.state import State
from data_sync.transport import Delivery
from tests.fakes import FakeS3, S3Error
from tests.test_transport import claim


def relative_source(config, state):
    source = config.files[0]
    source.id = "relative"
    source.path_layout = "relative"
    source.filename_regex = r"(?P<batch_no>.+)"
    state.configure(config, 0)
    return source


def collect(config, state, source, start=10):
    for now in (start, start + 5, start + 15):
        FileCollector(config, state).scan(source, now)


def test_relative_paths_and_manifest_retry(config, state):
    source = relative_source(config, state)
    names = ["a.hdf", "FY3F/2026/b.hdf", "中文/数据.xml"]
    for name in names:
        path = source.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(name.encode())
    collect(config, state, source)
    batches = state.all("SELECT * FROM batches")
    keys = set()
    for batch in batches:
        manifest = json.loads(batch["manifest"])
        assert batch["manifest_key"] == "_data_sync/manifests/" + manifest["manifest_id"][7:] + ".json"
        keys.add(batch["manifest_key"])
        assert manifest["files"][0]["target_key"] in names
    assert len(keys) == 3

    class Store(FakeS3):
        fail = True

        def put_object(self, **kwargs):
            if self.fail and kwargs["Key"].startswith("_data_sync/"):
                raise S3Error("ServiceUnavailable")
            return super().put_object(**kwargs)

    store = Store()
    task = claim(state)
    Delivery(state, config.targets[0], store).run(task)
    assert state.one("SELECT status FROM tasks WHERE id=?", (task["id"],))[0] == "RETRY_WAIT"
    assert len(store.events) == 1
    recovered = State(config.agent.work_dir / "state.sqlite3")
    try:
        recovered.configure(config, 30)
        store.fail = False
        while True:
            task = claim(recovered)
            if task is None:
                break
            Delivery(recovered, config.targets[0], store).run(task)
        assert len(store.events) == 6
        assert store.events[-1][1] in keys
        assert all(row[0] == "COMMITTED" for row in recovered.all("SELECT status FROM tasks"))
    finally:
        recovered.close()


@pytest.mark.parametrize("different", [False, True])
def test_relative_cross_source_collision(config, state, different):
    source = relative_source(config, state)
    path = source.root / "a.hdf"
    path.write_bytes(b"first")
    collect(config, state, source)
    store = FakeS3()
    Delivery(state, config.targets[0], store).run(claim(state))
    source.id = "another"
    state.configure(config, 30)
    if different:
        path.write_bytes(b"second")
    collect(config, state, source, 40)
    Delivery(state, config.targets[0], store).run(claim(state))
    assert state.one("SELECT status FROM tasks ORDER BY id DESC")[0] == ("BLOCKED" if different else "COMMITTED")
    assert store.objects["a.hdf"][0] == b"first"
    assert store.events.count(("put", "a.hdf")) == 1


@pytest.mark.parametrize("name", ["_data_sync", "_data_sync/manifests/a.json"])
def test_reserved_path(config, state, name):
    source = relative_source(config, state)
    path = source.root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"reserved")
    collect(config, state, source)
    assert tuple(state.one("SELECT status,error FROM files")) == ("QUARANTINED", "RESERVED_PATH")
    assert not state.all("SELECT * FROM batches")


def test_legacy_identity_and_switch_baseline(config, state):
    old = json.loads(state.one("SELECT fingerprint FROM sources")[0])
    old.pop("path_layout")
    state.db.execute("UPDATE sources SET fingerprint=?", (json.dumps(old),))
    state.configure(config, 1)
    config.files[0].path_layout = "relative"
    with pytest.raises(ValueError, match="source identity changed"):
        state.configure(config, 2)
    source = relative_source(config, state)
    source.initial_scan = "new_only"
    (source.root / "old.hdf").write_bytes(b"old")
    FileCollector(config, state).scan(source, 10)
    (source.root / "new.hdf").write_bytes(b"new")
    collect(config, state, source, 20)
    assert state.one("SELECT status FROM files WHERE path='old.hdf'")[0] == "IGNORED"
    batch = state.one("SELECT manifest FROM batches")
    assert json.loads(batch[0])["files"][0]["target_key"] == "new.hdf"
