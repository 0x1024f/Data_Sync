import json
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from data_sync.config import MySQLSource
from data_sync.mysql import MySQLCollector, encode_value
from tests.fakes import FakeMySQL


def source(config, state, initial="existing_and_new"):
    value = MySQLSource(id="orders", host="localhost", user="env:DB_USER", password="env:DB_PASS",
                        database="production", table="orders", primary_key="id", fields=["id", "value"],
                        batch_size=2, initial_scan=initial, commit_order_guaranteed=True)
    config.mysql.append(value)
    state.configure(config, 1)
    return value


def test_paging_and_cursor(config, state):
    src = source(config, state)
    db = FakeMySQL([{"id": i, "value": Decimal("12.50")} for i in (1, 2, 3)])
    collector = MySQLCollector(config, state, lambda _: db)
    collector.poll(src, 10)
    assert state.one("SELECT cursor FROM checkpoints")[0] == 2
    collector.poll(src, 20)
    collector.poll(src, 30)
    assert state.one("SELECT cursor FROM checkpoints")[0] == 3
    assert state.one("SELECT count(*) FROM batches")[0] == 2
    batch = state.one("SELECT manifest FROM batches ORDER BY created")[0]
    assert json.loads(batch)["dataset"]["record_count"] == 2


def test_prepared_batch_survives_checkpoint_failure(config, state, monkeypatch):
    src = source(config, state)
    db = FakeMySQL([{"id": 1, "value": "x"}, {"id": 2, "value": "y"}])
    collector = MySQLCollector(config, state, lambda _: db)
    original = state.ready
    def fail(*args, **kwargs):
        raise OSError("simulated disk failure")
    monkeypatch.setattr(state, "ready", fail)
    with pytest.raises(OSError):
        collector.poll(src, 10)
    assert state.one("SELECT cursor FROM checkpoints")[0] == 0
    queries = len(db.queries)
    monkeypatch.setattr(state, "ready", original)
    collector.recover_prepared(src)
    assert len(db.queries) == queries
    assert state.one("SELECT cursor FROM checkpoints")[0] == 2
    assert state.one("SELECT count(*) FROM batches")[0] == 1


def test_new_only_mysql(config, state):
    src = source(config, state, "new_only")
    db = FakeMySQL([{"id": 10, "value": "existing"}])
    collector = MySQLCollector(config, state, lambda _: db)
    collector.poll(src, 10)
    assert state.one("SELECT count(*) FROM batches")[0] == 0
    db.rows.append({"id": 11, "value": None})
    collector.poll(src, 20)
    assert state.one("SELECT cursor FROM checkpoints")[0] == 11


def test_serialization():
    assert encode_value(None) is None
    assert encode_value(Decimal("0.123456789123456789"))["value"] == "0.123456789123456789"
    assert encode_value(b"\x00\xff")["value"] == "AP8="
    assert encode_value(date(2026, 1, 2))["value"] == "2026-01-02"
    assert encode_value(datetime(2026, 1, 2, 3, 4))["$type"] == "datetime"
    assert encode_value(timedelta(seconds=-1))["value"] == -1000000
