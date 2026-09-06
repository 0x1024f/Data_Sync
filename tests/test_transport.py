import json

from data_sync.state import State
from data_sync.transport import Delivery
from tests.fakes import FakeS3
from tests.test_files import ready_batch


def claim(state, target="sz"):
    state.db.execute("UPDATE tasks SET next_retry=0")
    return state.claim(target, "test-worker")


def test_manifest_last_and_repeat_idempotent(config, state):
    batch = ready_batch(config, state)
    store = FakeS3()
    task = claim(state)
    Delivery(state, config.targets[0], store).run(task)
    assert state.one("SELECT status FROM tasks")[0] == "COMMITTED"
    assert store.events[-1] == ("put", batch["manifest_key"])
    events = list(store.events)
    state.db.execute("UPDATE tasks SET status='PENDING'")
    Delivery(state, config.targets[0], store).run(claim(state))
    assert store.events == events


def test_failed_manifest_retries_without_resending_data(config, state):
    batch = ready_batch(config, state)
    store = FakeS3()
    store.fail_manifest = True
    Delivery(state, config.targets[0], store).run(claim(state))
    assert batch["manifest_key"] not in store.objects
    assert state.one("SELECT status FROM tasks")[0] == "RETRY_WAIT"
    store.fail_manifest = False
    Delivery(state, config.targets[0], store).run(claim(state))
    assert len(store.events) == 2


def test_multipart_resume_after_restart(config, state):
    target = config.targets[0]
    target.part_size = target.multipart_threshold = 5242880
    batch = ready_batch(config, state, b"a" * (2 * 5242880 + 3))
    store = FakeS3()
    store.fail_part = 2
    Delivery(state, target, store).run(claim(state))
    assert batch["manifest_key"] not in store.objects
    assert store.events == [("part", 1)]
    recovered = State(config.agent.work_dir / "state.sqlite3")
    try:
        recovered.configure(config, 40)
        store.fail_part = None
        Delivery(recovered, target, store).run(claim(recovered))
        assert store.events.count(("part", 1)) == 1
        assert recovered.one("SELECT status FROM tasks")[0] == "COMMITTED"
        assert not recovered.all("SELECT * FROM uploads")
    finally:
        recovered.close()


def test_lost_complete_response_recovers(config, state):
    config.targets[0].multipart_threshold = config.targets[0].part_size = 5242880
    ready_batch(config, state, b"b" * 5242881)
    store = FakeS3()
    store.drop_complete_response = True
    Delivery(state, config.targets[0], store).run(claim(state))
    assert state.one("SELECT status FROM tasks")[0] == "RETRY_WAIT"
    Delivery(state, config.targets[0], store).run(claim(state))
    assert state.one("SELECT status FROM tasks")[0] == "COMMITTED"
    assert store.upload_number == 1


def test_conflict_blocks_without_manifest(config, state):
    batch = ready_batch(config, state)
    store = FakeS3()
    key = json.loads(batch["manifest"])["files"][0]["target_key"]
    store.objects[key] = (b"different", {"sha256": "wrong"})
    Delivery(state, config.targets[0], store).run(claim(state))
    assert state.one("SELECT status FROM tasks")[0] == "BLOCKED"
    assert batch["manifest_key"] not in store.objects


def test_targets_fail_independently(config, state):
    config.targets.append(config.targets[0].model_copy(update={"id": "b", "host": "b.example.internal"}))
    state.configure(config, 1)
    ready_batch(config, state)
    failed = FakeS3()
    failed.failed = True
    Delivery(state, config.targets[0], failed).run(claim(state))
    Delivery(state, config.targets[1], FakeS3()).run(claim(state, "b"))
    assert state.one("SELECT status FROM tasks WHERE target='sz'")[0] == "RETRY_WAIT"
    assert state.one("SELECT status FROM tasks WHERE target='b'")[0] == "COMMITTED"
