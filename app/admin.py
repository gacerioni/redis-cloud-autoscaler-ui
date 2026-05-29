"""Admin actions: safe FLUSHDB + force-reset DB to baseline."""
from __future__ import annotations
import json
import logging
import shutil
import subprocess
from typing import Any

from . import config

logger = logging.getLogger("admin")

# Keys with this prefix are the autoscaler's Rule/Task documents — preserve
# them so a FLUSHDB doesn't kneecap the scaling logic.
_AUTOSCALER_PREFIX = "com.redis.autoscaler."


def _run(cmd: list[str], timeout: int = 15) -> tuple[bool, str, str]:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode == 0, r.stdout.strip(), r.stderr.strip()
    except subprocess.TimeoutExpired:
        return False, "", "timeout"
    except Exception as e:
        return False, "", f"{type(e).__name__}: {e}"


def _redis_cli(args: list[str], timeout: int = 8) -> tuple[bool, str]:
    if not shutil.which("redis-cli"):
        return False, "redis-cli not installed in container"
    base = [
        "-h", config.DB_HOST, "-p", config.DB_PORT,
        "-a", config.REDIS_PASSWORD, "--no-auth-warning",
    ]
    ok, out, err = _run(["redis-cli", *base, *args], timeout=timeout)
    return ok, (out if ok else (err or out))


def flushdb() -> dict[str, Any]:
    """Wipe customer keys; preserve any com.redis.autoscaler.* documents."""
    ok, before = _redis_cli(["DBSIZE"], timeout=4)
    try:
        n_before = int(before.split()[-1])
    except Exception:
        n_before = -1

    # SCAN keys → exclude autoscaler prefix → UNLINK in batches of 500.
    script = (
        f"redis-cli -h {config.DB_HOST} -p {config.DB_PORT} -a '{config.REDIS_PASSWORD}' "
        f"--no-auth-warning --scan "
        f"| grep -v '^{_AUTOSCALER_PREFIX}' "
        f"| xargs -r -n 500 redis-cli -h {config.DB_HOST} -p {config.DB_PORT} "
        f"-a '{config.REDIS_PASSWORD}' --no-auth-warning UNLINK"
    )
    ok, _, err = _run(["bash", "-c", script], timeout=60)
    if not ok:
        return {"ok": False, "message": (err or "flush failed")[:200]}

    ok, after = _redis_cli(["DBSIZE"], timeout=4)
    try:
        n_after = int(after.split()[-1])
    except Exception:
        n_after = -1

    if n_before >= 0 and n_after >= 0:
        wiped = max(0, n_before - n_after)
        return {"ok": True, "message": f"Wiped {wiped:,} keys"}
    return {"ok": True, "message": "Flushed customer keys"}


def reset_to_baseline() -> dict[str, Any]:
    """PUT the DB back to baseline (throughput + memory) via the REST API."""
    payload = {
        "memoryLimitInGb": float(config.BASELINE_MEM_GB),
        "throughputMeasurement": {
            "by": "operations-per-second",
            "value": config.BASELINE_OPS,
        },
    }
    url = (f"{config.REDIS_CLOUD_API_BASE}/subscriptions/"
           f"{config.REDIS_CLOUD_SUBSCRIPTION_ID}/databases/{config.DB_ID}")
    ok, out, err = _run([
        "curl", "-sS", "--max-time", "12", "-X", "PUT",
        "-H", f"x-api-key: {config.REDIS_CLOUD_ACCOUNT_KEY}",
        "-H", f"x-api-secret-key: {config.REDIS_CLOUD_API_KEY}",
        "-H", "Content-Type: application/json",
        "-d", json.dumps(payload),
        url,
    ], timeout=15)
    if not ok:
        return {"ok": False, "message": (err or out)[:300]}
    msg = (f"Scale request submitted (back to {config.BASELINE_OPS:,} ops/sec · "
           f"{config.BASELINE_MEM_GB} GB)")
    try:
        data = json.loads(out)
        tid = data.get("taskId") or ""
        if tid:
            msg += f" · task {str(tid)[:8]}"
    except Exception:
        pass
    return {"ok": True, "message": msg}


def reload_scaling_rules() -> dict[str, Any]:
    """Idempotent rule re-register (useful if someone wiped the autoscaler's storage)."""
    from . import bootstrap
    import asyncio
    try:
        return asyncio.run(bootstrap.register_scaling_rules())
    except RuntimeError:
        # If called from within a running loop, fall back to a thread
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            return ex.submit(lambda: asyncio.run(bootstrap.register_scaling_rules())).result()
