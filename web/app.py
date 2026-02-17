"""FastAPI web application for the IMAP → M365 Migration Tool."""

from __future__ import annotations

import asyncio
import csv
import io
import os
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

import yaml
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Ensure the project root is on sys.path
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.config import (
    AppConfig, SourceConfig, TargetConfig, MailboxMapping, Options,
    load_config, ConfigError, _resolve_mailbox_passwords,
)
from src.state import StateDB
from web.models import (
    FullConfigModel, SourceConfigModel, TargetConfigModel,
    MailboxModel, OptionsModel, JobStatus, StatsOverview,
)
from web.runner import (
    broadcaster, get_current_job, cancel_job,
    start_migration, start_repair, start_merge, start_dryrun,
    start_relocate, start_purge, start_fix_drafts, start_clean,
)

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

WEB_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"
CONFIG_PATH = Path(_PROJECT_ROOT) / "config.yaml"


@asynccontextmanager
async def lifespan(application: FastAPI):
    broadcaster.set_loop(asyncio.get_running_loop())
    yield


app = FastAPI(title="IMAP → M365 Migration Tool", version="1.0.0", lifespan=lifespan)
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ---------------------------------------------------------------------------
# Frontend (serves the SPA)
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# ---------------------------------------------------------------------------
# Configuration API
# ---------------------------------------------------------------------------

def _load_raw_yaml() -> dict:
    """Load the raw YAML configuration file."""
    if not CONFIG_PATH.exists():
        return {}
    with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _save_raw_yaml(raw: dict) -> None:
    """Write configuration back to YAML."""
    with open(CONFIG_PATH, "w", encoding="utf-8") as fh:
        yaml.dump(raw, fh, default_flow_style=False, allow_unicode=True, sort_keys=False)


def _mask(s: str) -> str:
    """Mask a secret string for display."""
    if not s or len(s) < 6:
        return "***" if s else ""
    return s[:3] + "*" * (len(s) - 6) + s[-3:]


def _config_to_response(raw: dict) -> dict:
    """Convert raw YAML to an API-safe response with masked secrets."""
    src = raw.get("source", {})
    tgt = raw.get("target", {})
    opts = raw.get("options", {})
    mailboxes = raw.get("mailboxes", [])

    return {
        "source": {
            "host": src.get("host", ""),
            "port": src.get("port", 993),
            "ssl": src.get("ssl", True),
            "password": _mask(src.get("password", "")),
        },
        "target": {
            "tenant_id": tgt.get("tenant_id", ""),
            "client_id": tgt.get("client_id", ""),
            "client_secret": _mask(tgt.get("client_secret", "")),
        },
        "mailboxes": [
            {
                "id": i,
                "source_user": mb.get("source_user", ""),
                "target_user": mb.get("target_user", ""),
                "source_password": _mask(mb.get("source_password", "")),
                "include_folders": mb.get("include_folders"),
                "exclude_folders": mb.get("exclude_folders"),
            }
            for i, mb in enumerate(mailboxes)
        ],
        "options": {
            "batch_size": opts.get("batch_size", 50),
            "max_retries": opts.get("max_retries", 3),
            "max_workers": opts.get("max_workers", 4),
            "request_delay": opts.get("request_delay", 0.25),
            "log_level": opts.get("log_level", "INFO"),
            "exclude_folders": opts.get("exclude_folders") or [],
        },
    }


@app.get("/api/config")
async def get_config():
    raw = _load_raw_yaml()
    return _config_to_response(raw)


@app.put("/api/config/source")
async def update_source(model: SourceConfigModel):
    raw = _load_raw_yaml()
    src = raw.setdefault("source", {})
    src["host"] = model.host
    src["port"] = model.port
    src["ssl"] = model.ssl
    if model.password and "***" not in model.password:
        src["password"] = model.password
    _save_raw_yaml(raw)
    return {"status": "ok"}


@app.put("/api/config/target")
async def update_target(model: TargetConfigModel):
    raw = _load_raw_yaml()
    tgt = raw.setdefault("target", {})
    tgt["tenant_id"] = model.tenant_id
    tgt["client_id"] = model.client_id
    if model.client_secret and "***" not in model.client_secret:
        tgt["client_secret"] = model.client_secret
    _save_raw_yaml(raw)
    return {"status": "ok"}


@app.put("/api/config/options")
async def update_options(model: OptionsModel):
    raw = _load_raw_yaml()
    opts = raw.setdefault("options", {})
    opts["batch_size"] = model.batch_size
    opts["max_retries"] = model.max_retries
    opts["max_workers"] = model.max_workers
    opts["request_delay"] = model.request_delay
    opts["log_level"] = model.log_level
    opts["exclude_folders"] = model.exclude_folders if model.exclude_folders else None
    _save_raw_yaml(raw)
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Mailbox API
# ---------------------------------------------------------------------------

@app.get("/api/mailboxes")
async def list_mailboxes():
    raw = _load_raw_yaml()
    mailboxes = raw.get("mailboxes", [])
    return [
        {
            "id": i,
            "source_user": mb.get("source_user", ""),
            "target_user": mb.get("target_user", ""),
            "source_password": _mask(mb.get("source_password", "")),
            "include_folders": mb.get("include_folders"),
            "exclude_folders": mb.get("exclude_folders"),
        }
        for i, mb in enumerate(mailboxes)
    ]


@app.post("/api/mailboxes")
async def add_mailbox(model: MailboxModel):
    raw = _load_raw_yaml()
    mailboxes = raw.setdefault("mailboxes", [])
    mailboxes.append({
        "source_user": model.source_user,
        "target_user": model.target_user,
        "source_password": model.source_password,
    })
    _save_raw_yaml(raw)
    return {"status": "ok", "id": len(mailboxes) - 1}


@app.put("/api/mailboxes/{mb_id}")
async def update_mailbox(mb_id: int, model: MailboxModel):
    raw = _load_raw_yaml()
    mailboxes = raw.get("mailboxes", [])
    if mb_id < 0 or mb_id >= len(mailboxes):
        raise HTTPException(404, "Mailbox not found")
    mb = mailboxes[mb_id]
    mb["source_user"] = model.source_user
    mb["target_user"] = model.target_user
    if model.source_password and "***" not in model.source_password:
        mb["source_password"] = model.source_password
    if model.include_folders is not None:
        mb["include_folders"] = model.include_folders
    if model.exclude_folders is not None:
        mb["exclude_folders"] = model.exclude_folders
    _save_raw_yaml(raw)
    return {"status": "ok"}


@app.delete("/api/mailboxes/{mb_id}")
async def delete_mailbox(mb_id: int):
    raw = _load_raw_yaml()
    mailboxes = raw.get("mailboxes", [])
    if mb_id < 0 or mb_id >= len(mailboxes):
        raise HTTPException(404, "Mailbox not found")
    mailboxes.pop(mb_id)
    _save_raw_yaml(raw)
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Migration control API
# ---------------------------------------------------------------------------

def _load_app_config() -> AppConfig:
    """Load and validate the full AppConfig from config.yaml."""
    try:
        return load_config(str(CONFIG_PATH))
    except ConfigError as exc:
        raise HTTPException(400, str(exc))
    except Exception as exc:
        raise HTTPException(500, f"Failed to load config: {exc}")


@app.post("/api/migration/stop")
async def api_stop_migration():
    if cancel_job():
        return {"status": "cancelling"}
    raise HTTPException(404, "No running job")


@app.get("/api/migration/status")
async def api_migration_status():
    job = get_current_job()
    if job is None:
        return {"running": False, "message": "No active job"}
    return {
        "job_id": job.job_id,
        "type": job.type,
        "running": job.running,
        "cancelling": job.cancelling,
        "mailbox": job.mailbox,
        "progress_pct": round(job.progress_pct, 1),
        "message": job.message,
        "error": job.error,
        "started_at": job.started_at,
        "stats": job.stats,
    }


# ---------------------------------------------------------------------------
# Repair & Merge
# ---------------------------------------------------------------------------

@app.post("/api/repair/start")
async def api_start_repair():
    config = _load_app_config()
    try:
        job = start_repair(config)
        return {"status": "started", "job_id": job.job_id}
    except RuntimeError as exc:
        raise HTTPException(409, str(exc))


@app.post("/api/merge/start")
async def api_start_merge():
    config = _load_app_config()
    try:
        job = start_merge(config)
        return {"status": "started", "job_id": job.job_id}
    except RuntimeError as exc:
        raise HTTPException(409, str(exc))


@app.post("/api/dryrun/start")
async def api_start_dryrun():
    config = _load_app_config()
    try:
        job = start_dryrun(config)
        return {"status": "started", "job_id": job.job_id}
    except RuntimeError as exc:
        raise HTTPException(409, str(exc))


@app.post("/api/fix-drafts/start")
async def api_start_fix_drafts():
    config = _load_app_config()
    try:
        job = start_fix_drafts(config)
        return {"status": "started", "job_id": job.job_id}
    except RuntimeError as exc:
        raise HTTPException(409, str(exc))


@app.post("/api/purge/start")
async def api_start_purge():
    config = _load_app_config()
    purgeable = [m for m in config.mailboxes if m.old_target_user]
    if not purgeable:
        raise HTTPException(400, "No mailbox mappings have 'old_target_user' set.")
    try:
        job = start_purge(config)
        return {"status": "started", "job_id": job.job_id}
    except RuntimeError as exc:
        raise HTTPException(409, str(exc))


@app.post("/api/relocate/start")
async def api_start_relocate():
    config = _load_app_config()
    relocatable = [m for m in config.mailboxes if m.old_target_user]
    if not relocatable:
        raise HTTPException(
            400,
            "No mailbox mappings have 'old_target_user' set. "
            "Add old_target_user to each mapping that needs relocation in config.yaml.",
        )
    try:
        job = start_relocate(config)
        return {"status": "started", "job_id": job.job_id}
    except RuntimeError as exc:
        raise HTTPException(409, str(exc))


# ---------------------------------------------------------------------------
# Statistics & History
# ---------------------------------------------------------------------------

@app.get("/api/stats")
async def api_stats():
    db_path = Path(_PROJECT_ROOT) / "migration_state.db"
    if not db_path.exists():
        return StatsOverview().model_dump()

    state = StateDB(str(db_path))
    try:
        counts = state.get_stats()
        migrated = counts.get("success", 0)
        failed = counts.get("failed", 0)
        skipped = counts.get("skipped", 0)
        total = migrated + failed + skipped

        # Get run history
        with state._cursor() as cur:
            cur.execute("SELECT COUNT(*) as cnt FROM runs")
            total_runs = cur.fetchone()["cnt"]
            cur.execute("SELECT * FROM runs ORDER BY id DESC LIMIT 1")
            last_run = cur.fetchone()

        raw = _load_raw_yaml()
        total_mailboxes = len(raw.get("mailboxes", []))

        return {
            "total_mailboxes": total_mailboxes,
            "total_messages": total,
            "migrated": migrated,
            "failed": failed,
            "skipped": skipped,
            "total_runs": total_runs,
            "last_run_status": last_run["status"] if last_run else "",
            "last_run_date": last_run["started_at"] if last_run else "",
        }
    finally:
        state.close()


@app.get("/api/history")
async def api_history():
    db_path = Path(_PROJECT_ROOT) / "migration_state.db"
    if not db_path.exists():
        return []

    state = StateDB(str(db_path))
    try:
        with state._cursor() as cur:
            cur.execute("SELECT * FROM runs ORDER BY id DESC LIMIT 50")
            runs = [dict(row) for row in cur.fetchall()]
        return runs
    finally:
        state.close()


@app.get("/api/history/{run_id}")
async def api_run_detail(run_id: int):
    db_path = Path(_PROJECT_ROOT) / "migration_state.db"
    if not db_path.exists():
        raise HTTPException(404, "No state database")

    state = StateDB(str(db_path))
    try:
        with state._cursor() as cur:
            cur.execute("SELECT * FROM runs WHERE id = ?", (run_id,))
            run = cur.fetchone()
            if not run:
                raise HTTPException(404, "Run not found")

            cur.execute(
                "SELECT mailbox, folder, status, COUNT(*) as cnt "
                "FROM messages GROUP BY mailbox, folder, status ORDER BY mailbox, folder"
            )
            rows = [dict(r) for r in cur.fetchall()]

        return {"run": dict(run), "messages": rows}
    finally:
        state.close()


@app.get("/api/stats/mailbox/{mailbox}")
async def api_mailbox_stats(mailbox: str):
    db_path = Path(_PROJECT_ROOT) / "migration_state.db"
    if not db_path.exists():
        return {}

    state = StateDB(str(db_path))
    try:
        counts = state.get_stats(mailbox)
        with state._cursor() as cur:
            cur.execute(
                "SELECT folder, status, COUNT(*) as cnt "
                "FROM messages WHERE mailbox = ? GROUP BY folder, status ORDER BY folder",
                (mailbox,),
            )
            folders = [dict(r) for r in cur.fetchall()]
        return {"counts": counts, "folders": folders}
    finally:
        state.close()


# ---------------------------------------------------------------------------
# Per-mailbox folder stats
# ---------------------------------------------------------------------------

@app.get("/api/stats/folders")
async def api_all_folder_stats():
    """Return per-folder message counts grouped by mailbox and status."""
    db_path = Path(_PROJECT_ROOT) / "migration_state.db"
    if not db_path.exists():
        return []

    state = StateDB(str(db_path))
    try:
        with state._cursor() as cur:
            cur.execute(
                "SELECT mailbox, folder, status, COUNT(*) as cnt "
                "FROM messages GROUP BY mailbox, folder, status "
                "ORDER BY mailbox, folder, status"
            )
            rows = [dict(r) for r in cur.fetchall()]

        # Reshape into { mailbox: { folder: { success: N, failed: N } } }
        result: dict[str, dict[str, dict[str, int]]] = {}
        for r in rows:
            mb = result.setdefault(r["mailbox"], {})
            folder = mb.setdefault(r["folder"], {})
            folder[r["status"]] = r["cnt"]

        return [
            {
                "mailbox": mailbox,
                "folders": [
                    {"name": fname, **counts}
                    for fname, counts in sorted(folders.items())
                ],
            }
            for mailbox, folders in sorted(result.items())
        ]
    finally:
        state.close()


# ---------------------------------------------------------------------------
# CSV Export
# ---------------------------------------------------------------------------

@app.get("/api/export/csv")
async def api_export_csv():
    """Export migration summary as a downloadable CSV."""
    db_path = Path(_PROJECT_ROOT) / "migration_state.db"
    if not db_path.exists():
        raise HTTPException(404, "No migration state database found")

    state = StateDB(str(db_path))
    try:
        with state._cursor() as cur:
            cur.execute(
                "SELECT mailbox, folder, status, COUNT(*) as cnt "
                "FROM messages GROUP BY mailbox, folder, status "
                "ORDER BY mailbox, folder, status"
            )
            rows = [dict(r) for r in cur.fetchall()]
    finally:
        state.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Mailbox", "Folder", "Status", "Count"])
    for r in rows:
        writer.writerow([r["mailbox"], r["folder"], r["status"], r["cnt"]])
    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=migration_report.csv"},
    )


# ---------------------------------------------------------------------------
# Mailbox-filtered operations
# ---------------------------------------------------------------------------

@app.post("/api/migration/start")
async def api_start_migration(mailboxes: list[str] = Query(default=[])):
    config = _load_app_config()
    if mailboxes:
        selected = {m.lower() for m in mailboxes}
        config = _apply_mailbox_filter(config, selected)
    try:
        job = start_migration(config)
        return {"status": "started", "job_id": job.job_id}
    except RuntimeError as exc:
        raise HTTPException(409, str(exc))


@app.post("/api/clean/start")
async def api_start_clean(mailboxes: list[str] = Query(default=[])):
    config = _load_app_config()
    if mailboxes:
        selected = {m.lower() for m in mailboxes}
        config = _apply_mailbox_filter(config, selected)
    try:
        job = start_clean(config)
        return {"status": "started", "job_id": job.job_id}
    except RuntimeError as exc:
        raise HTTPException(409, str(exc))


def _apply_mailbox_filter(config: AppConfig, selected: set[str]) -> AppConfig:
    """Return a copy of config with only the selected mailboxes."""
    from dataclasses import replace
    filtered = [m for m in config.mailboxes if m.source_user.lower() in selected]
    if not filtered:
        raise HTTPException(400, f"No mailboxes matched: {selected}")
    return replace(config, mailboxes=filtered)


# ---------------------------------------------------------------------------
# Logs API
# ---------------------------------------------------------------------------

@app.get("/api/logs")
async def api_logs(lines: int = 200, level: str = ""):
    log_path = Path(_PROJECT_ROOT) / "migration.log"
    if not log_path.exists():
        return {"lines": []}

    with open(log_path, "r", encoding="utf-8", errors="replace") as fh:
        all_lines = fh.readlines()

    result = all_lines[-lines:]
    if level:
        level_upper = level.upper()
        result = [l for l in result if level_upper in l]

    return {"lines": result}


@app.get("/api/logs/download")
async def api_logs_download():
    log_path = Path(_PROJECT_ROOT) / "migration.log"
    if not log_path.exists():
        raise HTTPException(404, "No log file found")

    with open(log_path, "r", encoding="utf-8", errors="replace") as fh:
        content = fh.read()

    return StreamingResponse(
        iter([content]),
        media_type="text/plain",
        headers={"Content-Disposition": "attachment; filename=migration.log"},
    )


@app.get("/api/logs/recent")
async def api_recent_logs():
    return broadcaster.recent_events(200)


# ---------------------------------------------------------------------------
# WebSocket for real-time events
# ---------------------------------------------------------------------------

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await broadcaster.connect(ws)
    try:
        while True:
            # Keep the connection alive; incoming messages are ignored
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        broadcaster.disconnect(ws)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main():
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    host = os.environ.get("HOST", "0.0.0.0")
    uvicorn.run("web.app:app", host=host, port=port, reload=False, log_level="info")


if __name__ == "__main__":
    main()
