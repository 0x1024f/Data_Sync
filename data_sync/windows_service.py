"""Windows SCM entrypoint; config is fixed alongside the deployed service EXE."""
import sys
import threading
from pathlib import Path

if sys.platform == "win32":
    import servicemanager
    import win32service
    import win32serviceutil

    class DataSyncService(win32serviceutil.ServiceFramework):
        _svc_name_ = "DataSyncAgent"
        _svc_display_name_ = "Data Sync Agent"
        _svc_description_ = "Durable file and MySQL batch upload through SNI relay"

        def __init__(self, args):
            super().__init__(args)
            self.stop_event = threading.Event()

        def SvcStop(self):
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING, waitHint=120000)
            self.stop_event.set()

        def SvcDoRun(self):
            from data_sync.config import load_config
            from data_sync.runtime import Agent
            base = Path(sys.executable).parent if getattr(sys, "frozen", False) else Path(__file__).resolve().parents[1]
            try:
                Agent(load_config(base / "config.yaml"), self.stop_event).run()
            except Exception as error:
                servicemanager.LogErrorMsg("DataSyncAgent failed: " + type(error).__name__)
                self.ReportServiceStatus(win32service.SERVICE_STOPPED, win32ExitCode=1066, svcExitCode=1)
                raise


def main():
    if sys.platform != "win32":
        raise RuntimeError("Windows Service requires Windows")
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(DataSyncService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(DataSyncService)


if __name__ == "__main__":
    main()
