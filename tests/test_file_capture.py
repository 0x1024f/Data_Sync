import json
import os
from pathlib import Path

import pytest

from data_sync.files import FileCollector


def scan(config, state, *times):
    for now in times:
        FileCollector(config, state).scan(config.files[0], now)


def keys(state):
    return [r[0] for r in state.all("SELECT object_key FROM batches ORDER BY object_key")]


def test_independent_files_and_matching(config, state):
    source = config.files[0]
    source.include = ["**/*.dat"]
    source.exclude = ["**/skip.dat"]
    for name in ["a.dat", "中文/a.dat", "x/y/a.dat", "skip.dat", "x/skip.dat", "a.xml"]:
        path = source.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"hello")
    scan(config, state, 0)
    (source.root / "a.dat").write_bytes(b"changing")
    scan(config, state, 4, 5)
    assert keys(state) == ["x/y/a.dat", "中文/a.dat"]
    scan(config, state, 9)
    assert keys(state) == ["a.dat", "x/y/a.dat", "中文/a.dat"]
    assert all(r[0] is None for r in state.all("SELECT manifest_key FROM batches"))


def test_nonrecursive(config, state):
    config.files[0].recursive = False
    root = config.files[0].root
    (root / "nested").mkdir()
    (root / "nested/a").write_bytes(b"a")
    (root / "a").write_bytes(b"a")
    scan(config, state, 0, 5)
    assert keys(state) == ["a"]


def test_new_only_changes_and_same_content(config, state):
    source = config.files[0]
    source.initial_scan = "new_only"
    path = source.root / "old"
    path.write_bytes(b"old")
    scan(config, state, 0, 5)
    assert not keys(state)
    path.write_bytes(b"replacement")
    scan(config, state, 6, 11)
    assert keys(state) == ["old"]
    os.utime(path, ns=(path.stat().st_atime_ns, path.stat().st_mtime_ns + 1000000000))
    scan(config, state, 12, 17)
    assert keys(state) == ["old"]


def test_read_failure_does_not_block_other_files(config, state, monkeypatch):
    root = config.files[0].root
    for name in ("a", "b"):
        (root / name).write_bytes(b"x")
    scan(config, state, 0)
    original = Path.open
    def fail(path, *args, **kwargs):
        if path == root / "a" and args == ("rb",):
            raise PermissionError("denied")
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "open", fail)
    scan(config, state, 5)
    assert keys(state) == ["b"]
    assert len(list((config.agent.work_dir / "spool").iterdir())) == 1
    assert not list((config.agent.work_dir / "spool").rglob("*.tmp"))
    monkeypatch.setattr(Path, "open", original)
    scan(config, state, 6)
    assert keys(state) == ["a", "b"]


def test_mutation_during_snapshot_retries(config, state, monkeypatch):
    path = config.files[0].root / "a"
    path.write_bytes(b"old")
    scan(config, state, 0)
    original = os.fsync
    def mutate(fd):
        original(fd)
        path.write_bytes(b"new content")
    monkeypatch.setattr(os, "fsync", mutate)
    scan(config, state, 5)
    assert not keys(state)
    monkeypatch.setattr(os, "fsync", original)
    scan(config, state, 10)
    assert keys(state) == ["a"]
    capture = state.one("SELECT local_files FROM batches")[0]
    assert Path(json.loads(capture)["a"]).read_bytes() == b"new content"


def test_links_skipped_without_requiring_os_link_privileges(config, state, monkeypatch):
    root = config.files[0].root
    (root / "linked-file").write_bytes(b"skip")
    (root / "linked-dir").mkdir()
    (root / "linked-dir" / "nested").write_bytes(b"skip")
    (root / "empty").write_bytes(b"")
    original = Path.is_symlink
    monkeypatch.setattr(Path, "is_symlink", lambda path: path.name.startswith("linked-") or original(path))
    scan(config, state, 0, 5)
    assert keys(state) == ["empty"]
