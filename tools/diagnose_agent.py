"""Run the source agent with exception locations, without exception values."""
import json
import logging
from pathlib import Path
import sys
import traceback

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_sync.cli import main


class DiagnosticHandler(logging.Handler):
    def emit(self, record):
        error = sys.exc_info()[1]
        if error is None:
            return
        frames = traceback.extract_tb(error.__traceback__)
        print(json.dumps({
            "event": "diagnostic_exception",
            "error": type(error).__name__,
            "frames": [
                {"file": Path(frame.filename).name,
                 "line": frame.lineno, "function": frame.name}
                for frame in frames
            ],
        }), flush=True)


if __name__ == "__main__":
    logging.getLogger("data_sync").addHandler(DiagnosticHandler())
    raise SystemExit(main())
