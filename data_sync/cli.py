import argparse
import json
import signal
import sys
from pathlib import Path

from .config import load_config, secret
from .runtime import Agent, ProcessLock
from .state import State


def main(argv=None):
    parser = argparse.ArgumentParser(description="Data Sync Windows/source agent")
    parser.add_argument("--config", type=Path, required=True)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("validate", help="Validate configuration and secret references")
    sub.add_parser("run", help="Run until stopped")
    sub.add_parser("once", help="Scan/poll once and deliver currently ready tasks")
    sub.add_parser("status", help="Print durable ledger status")
    retry = sub.add_parser("retry", help="Requeue one blocked/failed task; stop agent first")
    retry.add_argument("task_id", type=int)
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        if args.command == "validate":
            for target in config.targets:
                if target.enabled:
                    secret(target.access_key)
                    secret(target.secret_key)
                    if target.ca_bundle and not target.ca_bundle.is_file():
                        raise ValueError("CA bundle missing")
            for source in config.mysql:
                secret(source.user)
                secret(source.password)
            print("Configuration and secret references valid")
        elif args.command in ("run", "once"):
            agent = Agent(config)
            for sig in (signal.SIGINT, signal.SIGTERM):
                signal.signal(sig, lambda *_: agent.stop.set())
            agent.run(once=args.command == "once")
        else:
            path = config.agent.work_dir / "state.sqlite3"
            if not path.exists():
                raise ValueError("state not initialized")
            if args.command == "status":
                # Read-only SQLite connection; no migration, lease or configuration changes.
                import sqlite3
                state = object.__new__(State)
                state.db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
                state.db.row_factory = sqlite3.Row
                try:
                    print(json.dumps(state.health(), ensure_ascii=False, indent=2))
                finally:
                    state.close()
            else:
                with ProcessLock(config.agent.work_dir / "agent.lock"):
                    state = State(path)
                    try:
                        result = state.db.execute("UPDATE tasks SET status='PENDING',next_retry=0,owner=NULL,lease_until=NULL,error=NULL WHERE id=? AND status IN ('BLOCKED','RETRY_WAIT')", (args.task_id,))
                        if not result.rowcount:
                            raise ValueError("task is missing or not retryable")
                    finally:
                        state.close()
        return 0
    except Exception as error:
        # Pydantic and SDK exceptions can echo secret-bearing input. Expose field
        # locations/type only; never print raw input or connection error messages.
        from pydantic import ValidationError
        if isinstance(error, ValidationError):
            errors = [{"field": ".".join(str(x) for x in e["loc"]), "type": e["type"]} for e in error.errors(include_input=False, include_context=False)]
            print(json.dumps({"error": "configuration_invalid", "fields": errors}), file=sys.stderr)
        else:
            print(json.dumps({"error": type(error).__name__}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
