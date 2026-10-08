import json
from pathlib import Path

import pytest

from data_sync.files import FileCollector
from data_sync.state import State
from data_sync.transport import Delivery
from tests.fakes import FakeS3
from tests.test_files import ready_batch
from tests.test_mysql import ready_mysql
from tests.test_transport import claim


def update(config, state, content, start=20):
    (config.files[0].root / "batch1_primary.dat").write_bytes(content)
    for now in (start, start + 5):
        FileCollector(config, state).scan(config.files[0], now)


def test_versions_serialize_across_workers_and_retry(config, state):
    ready_batch(config, state, b"first")
    update(config, state, b"second")
    first = claim(state)
    assert claim(state) is None
    store = FakeS3()
    store.failed = True
    Delivery(state, config.targets[0], store).run(first)
    state.db.execute("UPDATE tasks SET next_retry=9999999999 WHERE id=?", (first["id"],))
    assert state.claim("sz", "other-worker") is None
    state.db.execute("UPDATE tasks SET status='BLOCKED' WHERE id=?", (first["id"],))
    assert state.claim("sz", "other-worker") is None
    state.db.execute("UPDATE tasks SET status='PENDING',next_retry=0 WHERE id=?", (first["id"],))
    store.failed = False
    Delivery(state, config.targets[0], store).run(claim(state))
    assert store.objects["batch1_primary.dat"][0] == b"first"
    Delivery(state, config.targets[0], store).run(claim(state))
    assert store.objects["batch1_primary.dat"][0] == b"second"
    assert claim(state) is None


def test_multipart_overwrite_resume_existing_object(config, state):
    target = config.targets[0]
    target.part_size = target.multipart_threshold = 5242880
    ready_batch(config, state, b"first")
    store = FakeS3()
    Delivery(state, target, store).run(claim(state))
    content = b"x" * (5242880 + 1)
    update(config, state, content)
    store.fail_part = 2
    Delivery(state, target, store).run(claim(state))
    assert store.objects["batch1_primary.dat"][0] == b"first"
    recovered = State(config.agent.work_dir / "state.sqlite3")
    try:
        recovered.configure(config, 30)
        store.fail_part = None
        store.drop_complete_response = True
        Delivery(recovered, target, store).run(claim(recovered))
        assert recovered.one("SELECT status FROM tasks ORDER BY id DESC")[0] == "RETRY_WAIT"
        Delivery(recovered, target, store).run(claim(recovered))
        assert recovered.one("SELECT status FROM tasks ORDER BY id DESC")[0] == "COMMITTED"
        assert store.events.count(("part", 1)) == 1
        assert store.objects["batch1_primary.dat"][0] == content
        assert len(store.objects) == 1
    finally:
        recovered.close()


@pytest.mark.parametrize("file_first", [False, True])
@pytest.mark.parametrize("manifest_collision", [False, True])
def test_mysql_file_key_collision(config, state, file_first, manifest_collision):
    mysql = ready_mysql(config, state)
    key = mysql["manifest_key"] if manifest_collision else json.loads(mysql["manifest"])["files"][0]["target_key"]
    path = config.files[0].root / key
    path.parent.mkdir(parents=True)
    path.write_bytes(b"file")
    for now in (20, 25):
        FileCollector(config, state).scan(config.files[0], now)
    mysql_task = claim(state)
    file_task = claim(state)
    store = FakeS3()
    first, second = (file_task, mysql_task) if file_first else (mysql_task, file_task)
    Delivery(state, config.targets[0], store).run(first)
    before = dict(store.objects)
    Delivery(state, config.targets[0], store).run(second)
    assert state.one("SELECT status FROM tasks WHERE id=?", (second["id"],))[0] == "BLOCKED"
    assert store.objects == before


def test_failed_target_does_not_hold_other_target_versions(config, state):
    config.targets.append(config.targets[0].model_copy(update={"id": "b", "host": "b.example.internal"}))
    state.configure(config, 1)
    ready_batch(config, state, b"first")
    update(config, state, b"second")
    failed, good = FakeS3(), FakeS3()
    failed.failed = True
    Delivery(state, config.targets[0], failed).run(claim(state))
    for _ in range(2):
        Delivery(state, config.targets[1], good).run(claim(state, "b"))
    assert good.objects["batch1_primary.dat"][0] == b"second"
    assert state.one("SELECT count(*) FROM tasks WHERE target='b' AND status='COMMITTED'")[0] == 2
