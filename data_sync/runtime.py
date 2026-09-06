"""Process locking, structured rotating logs and independent target workers."""
import json
import logging
import os
import signal
import threading
import time
import uuid
from logging.handlers import RotatingFileHandler

from .files import FileCollector
from .mysql import MySQLCollector
from .state import State
from .transport import Delivery


class ProcessLock:
    def __init__(self, path):
        self.path, self.handle = path, None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+b")
        try:
            if os.name == "nt":
                import msvcrt
                self.handle.seek(0, 2)
                if not self.handle.tell():
                    self.handle.write(b"0")
                    self.handle.flush()
                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.handle.close()
            raise RuntimeError("another agent or maintenance command owns the work directory") from None
        return self

    def __exit__(self, *args):
        self.handle.close()


class JsonFormatter(logging.Formatter):
    def format(self, record):
        data = {"time": time.time(), "level": record.levelname, "event": record.getMessage()}
        for field in ("source", "target", "task", "code"):
            if hasattr(record, field):
                data[field] = getattr(record, field)
        return json.dumps(data, ensure_ascii=False)


def setup_logging(config):
    directory = config.agent.work_dir / "logs"
    directory.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(directory / "agent.jsonl", maxBytes=10485760, backupCount=5, encoding="utf-8")
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger("data_sync")
    root.setLevel(config.agent.log_level)
    root.addHandler(handler)
    return handler


class Agent:
    def __init__(self, config, stop=None):
        self.config = config
        self.stop = stop if stop is not None else threading.Event()
        self.path = config.agent.work_dir / "state.sqlite3"
        self.log = logging.getLogger(__name__)

    def _worker(self, target, once):
        state = State(self.path)
        owner = uuid.uuid4().hex
        try:
            while not self.stop.is_set():
                task = state.claim(target.id, owner)
                if task:
                    try:
                        Delivery(state, target, stop=self.stop).run(task)
                    except Exception as error:
                        state.finish(task, "BLOCKED", type(error).__name__)
                        self.log.error("target_initialization_failed", extra={"target": target.id, "code": type(error).__name__})
                elif once:
                    return
                else:
                    self.stop.wait(1)
        except Exception as error:
            self.log.error("worker_failed", extra={"target": target.id, "code": type(error).__name__})
            self.stop.set()
            self.failed = True
        finally:
            state.close()

    def run(self, once=False):
        self.failed = False
        with ProcessLock(self.config.agent.work_dir / "agent.lock"):
            handler = setup_logging(self.config)
            state = State(self.path)
            workers = []
            try:
                state.configure(self.config)
                files, mysql = FileCollector(self.config, state, self.stop), MySQLCollector(self.config, state)
                next_poll = {}
                if not once:
                    workers = self._workers(once)
                while not self.stop.is_set():
                    now = time.time()
                    state.db.execute("INSERT OR REPLACE INTO settings VALUES ('heartbeat',?)", (str(now),))
                    for source in self.config.files:
                        if self.stop.is_set():
                            break
                        try:
                            files.scan(source, now)
                            state.db.execute("DELETE FROM settings WHERE key=?", ("error:" + source.id,))
                        except Exception as error:
                            state.db.execute("INSERT OR REPLACE INTO settings VALUES (?,?)", ("error:" + source.id, type(error).__name__))
                            self.log.error("file_scan_failed", extra={"source": source.id, "code": type(error).__name__})
                    for source in self.config.mysql:
                        if self.stop.is_set():
                            break
                        if now >= next_poll.get(source.id, 0):
                            try:
                                mysql.recover_prepared(source)
                                mysql.poll(source, now)
                                state.db.execute("DELETE FROM settings WHERE key=?", ("error:" + source.id,))
                            except Exception as error:
                                state.db.execute("INSERT OR REPLACE INTO settings VALUES (?,?)", ("error:" + source.id, type(error).__name__))
                                self.log.error("mysql_poll_failed", extra={"source": source.id, "code": type(error).__name__})
                            next_poll[source.id] = now + source.poll_seconds
                    if once:
                        workers = self._workers(once)
                        break
                    self.stop.wait(self.config.agent.scan_seconds)
            except BaseException:
                self.stop.set()
                raise
            finally:
                for worker in workers:
                    worker.join()
                state.close()
                logging.getLogger("data_sync").removeHandler(handler)
                handler.close()
            if self.failed:
                raise RuntimeError("upload worker failed")

    def _workers(self, once):
        workers = []
        for target in self.config.targets:
            if target.enabled:
                for _ in range(target.concurrency):
                    thread = threading.Thread(target=self._worker, args=(target, once), name="upload-" + target.id)
                    thread.start()
                    workers.append(thread)
        return workers
