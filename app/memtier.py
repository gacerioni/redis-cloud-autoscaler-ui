"""memtier_benchmark controller.

The binary is shipped inside the UI image, so we spawn it as a local
subprocess instead of orchestrating a Docker container. State is tracked
via a single Popen handle held by this module.
"""
from __future__ import annotations
import logging
import os
import shutil
import signal
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

from . import config

logger = logging.getLogger("memtier")

_LOG_PATH = Path("/tmp/memtier.log")
_proc_lock = threading.Lock()
_proc: subprocess.Popen | None = None
_started_at: float | None = None
_current_params: dict[str, Any] | None = None


def _binary() -> str:
    p = shutil.which("memtier_benchmark")
    if not p:
        raise RuntimeError("memtier_benchmark not found in PATH "
                           "(the UI container should ship it)")
    return p


def is_running() -> bool:
    with _proc_lock:
        return _proc is not None and _proc.poll() is None


def status() -> dict[str, Any]:
    with _proc_lock:
        running = _proc is not None and _proc.poll() is None
        started = _started_at
        params = _current_params
    if not running:
        return {"running": False, "status": "", "running_for": "", "params": params}
    elapsed = int(time.time() - started) if started else 0
    if elapsed < 60:
        run_for = f"Up {elapsed} seconds"
    elif elapsed < 3600:
        run_for = f"Up {elapsed // 60} minutes"
    else:
        run_for = f"Up {elapsed // 3600}h {(elapsed % 3600) // 60}m"
    return {"running": True, "status": run_for, "running_for": run_for, "params": params}


def start(params: dict[str, Any]) -> dict[str, Any]:
    global _proc, _started_at, _current_params
    required = {"threads", "clients", "pipeline", "ratio", "data_size",
                "key_minimum", "key_maximum", "test_time"}
    missing = required - set(params)
    if missing:
        return {"ok": False, "message": f"missing params: {sorted(missing)}"}

    # Replace any existing run
    stop()

    argv = [_binary(), *config.build_memtier_argv(params)]
    logger.info("starting memtier: %s", " ".join(argv[:6] + ["…"]))
    try:
        log_fp = open(_LOG_PATH, "wb")
        proc = subprocess.Popen(
            argv,
            stdout=log_fp, stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,  # decouple from uvicorn signals
        )
    except Exception as e:
        return {"ok": False, "message": f"{type(e).__name__}: {e}"}

    with _proc_lock:
        _proc = proc
        _started_at = time.time()
        _current_params = params
    return {"ok": True, "message": "started", "pid": proc.pid}


def stop() -> dict[str, Any]:
    global _proc, _started_at, _current_params
    with _proc_lock:
        proc = _proc
        _proc = None
        _started_at = None
    if proc is None or proc.poll() is not None:
        return {"ok": True, "message": "not running"}
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.wait(timeout=3)
    except ProcessLookupError:
        pass
    except Exception as e:
        return {"ok": False, "message": f"{type(e).__name__}: {e}"}
    return {"ok": True, "message": "stopped"}
