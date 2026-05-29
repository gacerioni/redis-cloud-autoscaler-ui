"""FastAPI entry point.

Runs the bootstrap (validate config + register scaling rules) at startup,
then exposes REST + WebSocket endpoints used by the UI.

Optional HTTP Basic Auth is applied to every route (including the WebSocket
handshake) when UI_AUTH_PASSWORD is set. Empty password = open access.
"""
from __future__ import annotations
import asyncio
import base64
import json
import logging
import secrets
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles

from . import admin, bootstrap, config, memtier
from .state import StateManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
)
logger = logging.getLogger("api")

state_mgr = StateManager()
STATIC_DIR = Path(__file__).resolve().parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1) Boot-time setup (idempotent, fail-soft)
    try:
        await bootstrap.run()
    except Exception as e:
        logger.exception("bootstrap error (continuing anyway): %s", e)
    # 2) Background fetcher
    bg = asyncio.create_task(state_mgr.run_forever())
    logger.info("UI ready")
    try:
        yield
    finally:
        bg.cancel()
        try:
            await bg
        except asyncio.CancelledError:
            pass
        memtier.stop()


# --------------------------------------------------------------------- auth (optional)
_security = HTTPBasic(auto_error=False)
_AUTH_ENABLED = bool(config.UI_AUTH_PASSWORD)
if _AUTH_ENABLED:
    logger.info("HTTP Basic Auth enabled (user=%s)", config.UI_AUTH_USERNAME)


def require_auth(credentials: HTTPBasicCredentials | None = Depends(_security)) -> None:
    if not _AUTH_ENABLED:
        return
    if not credentials or not (
        secrets.compare_digest(credentials.username, config.UI_AUTH_USERNAME) and
        secrets.compare_digest(credentials.password, config.UI_AUTH_PASSWORD)
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": 'Basic realm="autoscaler-ui"'},
        )


def _ws_auth_ok(websocket: WebSocket) -> bool:
    """Validate Basic credentials on the WebSocket upgrade headers.
    Browsers send these along automatically once the user has authed for the page."""
    if not _AUTH_ENABLED:
        return True
    raw = websocket.headers.get("authorization", "")
    if not raw.startswith("Basic "):
        return False
    try:
        decoded = base64.b64decode(raw.split(" ", 1)[1]).decode("utf-8", "ignore")
        user, _, pw = decoded.partition(":")
        return (secrets.compare_digest(user, config.UI_AUTH_USERNAME) and
                secrets.compare_digest(pw, config.UI_AUTH_PASSWORD))
    except Exception:
        return False


_app_deps = [Depends(require_auth)] if _AUTH_ENABLED else []
app = FastAPI(title="Redis Cloud Autoscaler UI", lifespan=lifespan, dependencies=_app_deps)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])


# --------------------------------------------------------------------- REST
@app.get("/api/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/state")
async def get_state() -> JSONResponse:
    return JSONResponse(state_mgr.snapshot())


@app.get("/api/config")
async def get_config() -> dict[str, Any]:
    return {
        "client_name":  config.CLIENT_NAME,
        "tagline":      config.CLIENT_TAGLINE,
        "db":           {"id": config.DB_ID,
                         "baseline_ops": config.BASELINE_OPS,
                         "burst_ops": config.BURST_OPS,
                         "baseline_mem_gb": config.BASELINE_MEM_GB,
                         "memory_ceiling_gb": config.MEMORY_CEILING_GB},
        "thresholds":   {"throughput_pct": config.THROUGHPUT_THRESHOLD_PCT,
                         "memory_pct":     config.MEMORY_THRESHOLD_PCT,
                         "throughput_for": config.THROUGHPUT_THRESHOLD_FOR,
                         "memory_for":     config.MEMORY_THRESHOLD_FOR},
        "presets":      config.PRESETS,
    }


# Memtier control
@app.post("/api/load/start")
async def start_load(params: dict[str, Any]) -> dict[str, Any]:
    res = await asyncio.get_event_loop().run_in_executor(None, memtier.start, params)
    return res


@app.post("/api/load/stop")
async def stop_load() -> dict[str, Any]:
    return await asyncio.get_event_loop().run_in_executor(None, memtier.stop)


@app.get("/api/load/status")
async def load_status() -> dict[str, Any]:
    return await asyncio.get_event_loop().run_in_executor(None, memtier.status)


# Admin
@app.post("/api/admin/flushdb")
async def admin_flushdb() -> dict[str, Any]:
    return await asyncio.get_event_loop().run_in_executor(None, admin.flushdb)


@app.post("/api/admin/reset-baseline")
async def admin_reset_baseline() -> dict[str, Any]:
    return await asyncio.get_event_loop().run_in_executor(None, admin.reset_to_baseline)


@app.post("/api/admin/reload-rules")
async def admin_reload_rules() -> dict[str, Any]:
    return await asyncio.get_event_loop().run_in_executor(None, admin.reload_scaling_rules)


# Auto-reset
@app.post("/api/auto-reset/cancel")
async def auto_reset_cancel() -> dict[str, Any]:
    return await state_mgr.cancel_auto_reset()


@app.post("/api/auto-reset/now")
async def auto_reset_now() -> dict[str, Any]:
    return await state_mgr.trigger_auto_reset_now()


# --------------------------------------------------------------------- WebSocket
@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket) -> None:
    if not _ws_auth_ok(websocket):
        await websocket.close(code=1008)  # policy violation
        return
    await websocket.accept()
    try:
        await websocket.send_text(json.dumps(state_mgr.snapshot()))
        while True:
            await asyncio.sleep(1.0)
            await websocket.send_text(json.dumps(state_mgr.snapshot()))
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.warning("WS error: %s", e)


# --------------------------------------------------------------------- static (must be LAST)
app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
