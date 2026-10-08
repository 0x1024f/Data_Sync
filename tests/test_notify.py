import io
import json
import threading
from urllib.error import HTTPError, URLError
from unittest.mock import Mock

import pytest

from data_sync.config import NotifyConfig
from data_sync.notify import NotificationError, post_notification
from data_sync.runtime import JsonFormatter
from data_sync.state import State
from data_sync.transport import Delivery
from tests.fakes import FakeS3
from tests.test_files import ready_batch
from tests.test_file_versions import update
from tests.test_mysql import ready_mysql


@pytest.fixture
def enabled(config):
    config.targets[0].notify = NotifyConfig(url="http://notify.test/notify", module_type="ecology",
                                            path_prefix="/rs-shared/")
    return config.targets[0]


def test_success_payload_and_verification(config, state, enabled):
    ready_batch(config, state)
    store = FakeS3()
    calls = []
    def send(url, payload, timeout):
        assert store.objects["batch1_primary.dat"][0] == b"data"
        calls.append((url, payload, timeout))
    Delivery(state, enabled, store, notifier=send).run(state.claim("sz", "worker"))
    assert calls == [(enabled.notify.url, {"moduleType": "ecology", "bucketName": "test-bucket",
                                          "filePathList": ["/rs-shared/batch1_primary.dat"]}, 30)]
    assert state.one("SELECT status FROM tasks")[0] == "COMMITTED"
    assert state.health()["notifications"] == [{"target": "sz", "status": "SUCCEEDED", "count": 1}]


def test_retry_timing_recovery_and_limit(config, state, enabled, monkeypatch, caplog):
    now = [1000.0]
    monkeypatch.setattr("data_sync.state.time.time", lambda: now[0])
    ready_batch(config, state)
    store = FakeS3()
    send = Mock(side_effect=NotificationError("BusinessFailure"))
    Delivery(state, enabled, store, notifier=send).run(state.claim("sz", "worker"))
    events = list(store.events)
    assert state.one("SELECT next_retry FROM notifications")[0] == 1030
    assert state.claim("sz", "worker") is None
    # Pending requests retain their saved URL/body even if config changes after restart.
    enabled.notify = None
    recovered = State(config.agent.work_dir / "state.sqlite3")
    try:
        recovered.configure(config)
        monkeypatch.setattr("data_sync.transport.client_for", Mock(side_effect=AssertionError("S3 must not be opened")))
        now[0] = 1030
        Delivery(recovered, enabled, notifier=send).run(recovered.claim("sz", "worker"))
        assert recovered.one("SELECT next_retry FROM tasks")[0] == 1210
        now[0] = 1209
        assert recovered.claim("sz", "worker") is None
        now[0] = 1210
        Delivery(recovered, enabled, notifier=send).run(recovered.claim("sz", "worker"))
        assert tuple(recovered.one("SELECT status,attempts,error FROM notifications")) == ("FAILED", 3, "BusinessFailure")
        assert recovered.one("SELECT status FROM tasks")[0] == "COMMITTED"
        assert recovered.claim("sz", "worker") is None
    finally:
        recovered.close()
    assert send.call_count == 3
    assert store.events == events
    assert "notification_exhausted" in caplog.text
    record = next(r for r in caplog.records if r.message == "notification_exhausted")
    assert json.loads(JsonFormatter().format(record))["notification_attempt"] == 3


@pytest.mark.parametrize("kind", ["mysql", "disabled", "upload_failed", "verify_failed"])
def test_no_early_or_unwanted_notification(config, state, enabled, kind):
    (ready_mysql if kind == "mysql" else ready_batch)(config, state)
    store = FakeS3()
    if kind == "disabled":
        enabled.notify = None
    if kind == "upload_failed":
        store.failed = True
    send = Mock()
    delivery = Delivery(state, enabled, store, notifier=send)
    if kind == "verify_failed":
        delivery._check = Mock(side_effect=[False, False, True, False])
    delivery.run(state.claim("sz", "worker"))
    send.assert_not_called()
    assert not state.all("SELECT * FROM notifications")


def test_same_key_versions_and_independent_targets(config, state, enabled, monkeypatch):
    now = [1000.0]
    monkeypatch.setattr("data_sync.state.time.time", lambda: now[0])
    config.targets.append(enabled.model_copy(update={"id": "b", "host": "b.example.internal"}))
    state.configure(config, 0)
    ready_batch(config, state, b"first")
    update(config, state, b"second")
    store = FakeS3()
    send = Mock(side_effect=[NotificationError("BusinessFailure"), None, None])
    Delivery(state, enabled, store, notifier=send).run(state.claim("sz", "worker"))
    assert state.claim("sz", "other") is None
    other = FakeS3()
    for _ in range(2):
        Delivery(state, config.targets[1], other, notifier=Mock()).run(state.claim("b", "other"))
    assert other.objects["batch1_primary.dat"][0] == b"second"
    assert store.objects["batch1_primary.dat"][0] == b"first"
    now[0] += 30
    for _ in range(2):
        Delivery(state, enabled, store, notifier=send).run(state.claim("sz", "worker"))
    assert store.objects["batch1_primary.dat"][0] == b"second"


def test_inflight_crash_respects_attempt_limit(config, state, enabled, monkeypatch):
    ready_batch(config, state)
    now = [1000.0]
    monkeypatch.setattr("data_sync.state.time.time", lambda: now[0])
    Delivery(state, enabled, FakeS3(), notifier=Mock(side_effect=NotificationError("BusinessFailure"))).run(state.claim("sz", "worker"))
    now[0] += 30
    task = state.claim("sz", "worker")
    state.start_notification(task)
    now[0] += 210
    state.configure(config)
    state.start_notification(state.claim("sz", "worker"))
    now[0] += 210
    state.configure(config)
    send = Mock()
    Delivery(state, enabled, notifier=send).run(state.claim("sz", "worker"))
    send.assert_not_called()
    assert tuple(state.one("SELECT status,attempts FROM notifications")) == ("FAILED", 3)
    assert state.one("SELECT status FROM tasks")[0] == "COMMITTED"


def test_stop_after_verification_preserves_pending(config, state, enabled):
    ready_batch(config, state)
    stop = threading.Event()
    store = FakeS3()
    delivery = Delivery(state, enabled, store, stop=stop, notifier=Mock())
    original = state.prepare_notification
    def prepare(*args):
        original(*args)
        stop.set()
    state.prepare_notification = prepare
    delivery.run(state.claim("sz", "worker"))
    assert tuple(state.one("SELECT status,attempts FROM notifications")) == ("PENDING", 0)
    delivery.notifier.assert_not_called()
    stop.clear()
    delivery.run(state.claim("sz", "worker"))
    delivery.notifier.assert_called_once()


def test_unrelated_file_progresses_and_exhaustion_releases_version(config, state, enabled, monkeypatch):
    from data_sync.files import FileCollector
    now = [1000.0]
    monkeypatch.setattr("data_sync.state.time.time", lambda: now[0])
    ready_batch(config, state, b"first")
    update(config, state, b"second")
    (config.files[0].root / "other.dat").write_bytes(b"other")
    for scan in (40, 45):
        FileCollector(config, state).scan(config.files[0], scan)
    store = FakeS3()
    failure = Mock(side_effect=NotificationError("BusinessFailure"))
    Delivery(state, enabled, store, notifier=failure).run(state.claim("sz", "worker"))
    Delivery(state, enabled, store, notifier=Mock()).run(state.claim("sz", "worker"))
    assert store.objects["other.dat"][0] == b"other"
    assert store.objects["batch1_primary.dat"][0] == b"first"
    for delay in (30, 180):
        now[0] += delay
        Delivery(state, enabled, store, notifier=failure).run(state.claim("sz", "worker"))
    Delivery(state, enabled, store, notifier=Mock()).run(state.claim("sz", "worker"))
    assert store.objects["batch1_primary.dat"][0] == b"second"


def test_v3_migration_no_backfill(config, state):
    ready_batch(config, state)
    Delivery(state, config.targets[0], FakeS3()).run(state.claim("sz", "worker"))
    state.db.execute("DROP TABLE notifications")
    state.db.execute("PRAGMA user_version=3")
    assert state.health()["notifications"] == []
    recovered = State(config.agent.work_dir / "state.sqlite3")
    try:
        assert recovered.one("PRAGMA user_version")[0] == 4
        assert recovered.one("SELECT status FROM tasks")[0] == "COMMITTED"
        assert not recovered.all("SELECT * FROM notifications")
    finally:
        recovered.close()


@pytest.mark.parametrize("status,body,error", [
    (200, b'{"code":200}', None), (201, b'{"code":200}', None),
    (200, b'{"code":"200"}', "BusinessFailure"), (200, b'{"code":500}', "BusinessFailure"),
    (200, b'{}', "BusinessFailure"), (200, b'[]', "BusinessFailure"),
    (204, b'', "InvalidJSON"), (200, b'not json', "InvalidJSON"),
    (500, b'{"code":200}', "HTTP_500"), (302, b'', "HTTP_302"),
    pytest.param(200, b'x' * (1024 * 1024 + 1), "ResponseTooLarge", id="response-too-large"),
])
def test_http_response_and_request(monkeypatch, status, body, error):
    response = Mock(status=status)
    response.read.return_value = body
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    opener = Mock()
    opener.open.return_value = response
    monkeypatch.setattr("data_sync.notify.build_opener", lambda *args: opener)
    payload = {"filePathList": ["/rs-shared/中文 文件.HDF"]}
    if error:
        with pytest.raises(NotificationError, match=error):
            post_notification("http://notify.test/notify", payload, 30)
    else:
        post_notification("http://notify.test/notify", payload, 30)
    request = opener.open.call_args.args[0]
    assert request.get_method() == "POST"
    assert request.get_header("Content-type") == "application/json"
    assert json.loads(request.data) == payload
    assert opener.open.call_args.kwargs == {"timeout": 30}


@pytest.mark.parametrize("error", [URLError("private"), TimeoutError("private"),
                                   HTTPError("http://notify.test", 503, "private", {}, io.BytesIO())])
def test_transport_errors_sanitized(monkeypatch, error):
    opener = Mock()
    opener.open.side_effect = error
    monkeypatch.setattr("data_sync.notify.build_opener", lambda *args: opener)
    with pytest.raises(NotificationError) as caught:
        post_notification("http://notify.test", {}, 30)
    assert "private" not in str(caught.value)


@pytest.mark.parametrize("prefix,expected", [("", ""), ("/", "/"), ("/rs-shared//", "/rs-shared"),
                                            ("中文 空格/", "中文 空格")])
def test_prefix_normalization(prefix, expected):
    assert NotifyConfig(url="http://notify.test", module_type="ecology", path_prefix=prefix).path_prefix == expected


@pytest.mark.parametrize("updates", [{"url": "ftp://notify.test"}, {"url": "http://u:p@notify.test"},
    {"url": "http://notify.test:99999"}, {"path_prefix": "../data"}, {"path_prefix": "C:\\data"},
    {"timeout_seconds": 0}, {"module_type": ""}])
def test_invalid_config(updates):
    with pytest.raises(ValueError):
        NotifyConfig.model_validate({"url": "http://notify.test", "module_type": "ecology", **updates})
