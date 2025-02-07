import signal
import subprocess
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

counter = int(sys.argv[1])


def open_ipc_subprocess(parts: Sequence[str], **kwargs: Any) -> "subprocess.Popen[Any]":
    creationflags = 0
    if sys.platform == "win32":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP

    return subprocess.Popen(
        parts,
        creationflags=creationflags,
        **kwargs,
    )


def interrupt_ipc_subprocess(proc: "subprocess.Popen[Any]") -> None:
    """Send CTRL_BREAK on Windows, SIGINT on other platforms."""
    if sys.platform == "win32":
        proc.send_signal(signal.CTRL_BREAK_EVENT)
    else:
        proc.send_signal(signal.SIGINT)


if counter > 0:
    proc = open_ipc_subprocess(
        [
            sys.executable,
            str(Path(__file__)),
            str(counter - 1),
        ]
    )
else:
    proc = None

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print(f"Starting shutdown {counter}")  # noqa: T201
    if proc:
        interrupt_ipc_subprocess(proc)
        proc.wait(timeout=10)
        proc.communicate()
    print(f"Finished shutdown {counter}")  # noqa: T201
