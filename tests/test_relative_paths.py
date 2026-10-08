import pytest

from data_sync.files import FileCollector
from data_sync.transport import Delivery
from tests.fakes import FakeS3
from tests.test_transport import claim


def collect(config, state, source, start=10):
    for now in (start, start + 5):
        FileCollector(config, state).scan(source, now)


def test_relative_paths_only(config, state):
    source = config.files[0]
    names = ["a.hdf", "FY3F/2026/a.hdf", "中文/数据.xml", "_data_sync/manifests/a.json"]
    for name in names:
        path = source.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(name.encode())
    collect(config, state, source)
    store = FakeS3()
    while True:
        task = claim(state)
        if task is None:
            break
        Delivery(state, config.targets[0], store).run(task)
    assert set(store.objects) == set(names)
    assert all(store.objects[name][0] == name.encode() for name in names)


@pytest.mark.parametrize("different", [False, True])
def test_relative_cross_source_collision(config, state, different):
    source = config.files[0]
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
    assert state.one("SELECT status FROM tasks ORDER BY id DESC")[0] == "BLOCKED"
    assert store.objects["a.hdf"][0] == b"first"
