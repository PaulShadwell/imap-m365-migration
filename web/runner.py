"""Background migration runner with WebSocket progress broadcasting.

This wraps the existing CLI migration engine and runs it in a background
thread.  A custom logging handler captures log messages and sends them to
all connected WebSocket clients so the frontend can display live progress.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from fastapi import WebSocket

# Make sure the project root is on sys.path so ``src.*`` imports work
# regardless of how the web app is started.
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.config import AppConfig, MailboxMapping, SourceConfig, Options, load_config
from src.graph_client import GraphClient
from src.imap_source import IMAPSource
from src.folder_mapper import FolderMapper
from src.mail_migrator import MailMigrator, MigrationStats
from src.state import StateDB
from src.logger import setup_logging, get_logger

log = get_logger("web.runner")


# ---------------------------------------------------------------------------
# WebSocket event broadcaster
# ---------------------------------------------------------------------------

class EventBroadcaster:
    """Fan-out WebSocket connections + in-memory event buffer."""

    def __init__(self) -> None:
        self._clients: set[WebSocket] = set()
        self._event_buffer: deque[dict] = deque(maxlen=1000)
        self._lock = threading.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # -- connection management --

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._clients.add(ws)

    def disconnect(self, ws: WebSocket) -> None:
        self._clients.discard(ws)

    def set_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    # -- broadcasting --

    def broadcast(self, event: dict) -> None:
        """Thread-safe: queues the event for async delivery to all clients."""
        with self._lock:
            self._event_buffer.append(event)

        if self._loop is None:
            return

        dead: list[WebSocket] = []
        for ws in list(self._clients):
            try:
                asyncio.run_coroutine_threadsafe(ws.send_json(event), self._loop)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._clients.discard(ws)

    def recent_events(self, n: int = 200) -> list[dict]:
        with self._lock:
            return list(self._event_buffer)[-n:]


broadcaster = EventBroadcaster()


# ---------------------------------------------------------------------------
# Logging handler that forwards to the broadcaster
# ---------------------------------------------------------------------------

class WebSocketLogHandler(logging.Handler):
    """Sends log records as WebSocket events so the UI can show live logs."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            broadcaster.broadcast({
                "event": "log",
                "level": record.levelname,
                "logger": record.name,
                "message": self.format(record),
                "ts": time.strftime("%H:%M:%S", time.localtime(record.created)),
            })
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Job tracking
# ---------------------------------------------------------------------------

@dataclass
class Job:
    job_id: str
    type: str  # "migrate", "repair", "merge", "dryrun"
    running: bool = True
    cancelling: bool = False
    started_at: str = ""
    mailbox: str = ""
    progress_pct: float = 0.0
    message: str = ""
    stats: dict = field(default_factory=dict)
    error: str = ""


_current_job: Optional[Job] = None
_job_lock = threading.Lock()


def get_current_job() -> Optional[Job]:
    return _current_job


def cancel_job() -> bool:
    """Signal the current job to stop."""
    if _current_job and _current_job.running:
        _current_job.cancelling = True
        broadcaster.broadcast({"event": "cancelling"})
        return True
    return False


# ---------------------------------------------------------------------------
# Generic job launcher
# ---------------------------------------------------------------------------

def _launch(job_type: str, target: callable, args: tuple) -> Job:
    global _current_job
    with _job_lock:
        if _current_job and _current_job.running:
            raise RuntimeError("A job is already running")
        job = Job(
            job_id=str(uuid.uuid4()),
            type=job_type,
            started_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        )
        _current_job = job

    thread = threading.Thread(target=target, args=(job, *args), daemon=True)
    thread.start()
    return job


# ---------------------------------------------------------------------------
# Install WebSocket log handler
# ---------------------------------------------------------------------------

_ws_handler_installed = False

def _ensure_ws_handler() -> None:
    """Attach the WebSocket log handler to the 'migration' logger only.

    Adding it to the root logger would cause duplicates because messages
    from ``migration.*`` loggers propagate up to ``migration`` (which
    handles them) and then again to root (which would handle them a
    second time).
    """
    global _ws_handler_installed
    if _ws_handler_installed:
        return
    handler = WebSocketLogHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logging.getLogger("migration").addHandler(handler)
    _ws_handler_installed = True


# ---------------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------------

def start_migration(config: AppConfig) -> Job:
    return _launch("migrate", _run_migration, (config,))


def _run_migration(job: Job, config: AppConfig) -> None:
    try:
        _ensure_ws_handler()
        setup_logging(level=config.options.log_level, log_file=config.options.log_file)

        state = StateDB(config.options.state_db)
        graph = GraphClient(
            config.target,
            max_retries=config.options.max_retries,
            request_delay=config.options.request_delay,
        )
        migrator = MailMigrator(config.source, graph, state, config.options)
        run_id = state.start_run()

        all_stats: list[dict] = []
        total_mb = len(config.mailboxes)

        for i, mapping in enumerate(config.mailboxes):
            if job.cancelling:
                break

            label = f"{mapping.source_user} → {mapping.target_user}"
            job.mailbox = label
            job.message = f"Migrating {label} ({i+1}/{total_mb})"
            job.progress_pct = (i / total_mb) * 100
            broadcaster.broadcast({
                "event": "mailbox_start",
                "mailbox": label,
                "index": i,
                "total": total_mb,
            })

            try:
                stats = migrator.migrate_mailbox(mapping)
                all_stats.append({
                    "mailbox": label,
                    "folders": stats.folders_processed,
                    "total": stats.messages_total,
                    "migrated": stats.messages_migrated,
                    "skipped": stats.messages_skipped,
                    "failed": stats.messages_failed,
                    "success_rate": round(stats.success_rate, 1),
                })
            except Exception as exc:
                log.error("Mailbox %s failed: %s", label, exc, exc_info=True)
                broadcaster.broadcast({
                    "event": "error",
                    "mailbox": label,
                    "message": str(exc)[:500],
                })

            broadcaster.broadcast({
                "event": "mailbox_done",
                "mailbox": label,
                "index": i,
                "total": total_mb,
            })

        status = "interrupted" if job.cancelling else "completed"
        state.end_run(run_id, status)
        state.close()
        job.stats = {"runs": all_stats}
        job.progress_pct = 100
        job.message = status.capitalize()
        broadcaster.broadcast({"event": "complete", "status": status, "stats": all_stats})

    except Exception as exc:
        log.error("Migration failed: %s", exc, exc_info=True)
        job.error = str(exc)[:500]
        broadcaster.broadcast({"event": "error", "message": str(exc)[:500]})
    finally:
        job.running = False


# ---------------------------------------------------------------------------
# Repair
# ---------------------------------------------------------------------------

def start_repair(config: AppConfig) -> Job:
    return _launch("repair", _run_repair, (config,))


def _run_repair(job: Job, config: AppConfig) -> None:
    try:
        _ensure_ws_handler()
        setup_logging(level=config.options.log_level, log_file=config.options.log_file)
        state = StateDB(config.options.state_db)
        graph = GraphClient(
            config.target,
            max_retries=config.options.max_retries,
            request_delay=config.options.request_delay,
        )

        from src.mail_migrator import _parse_imap_date

        total_mb = len(config.mailboxes)
        for mi, mapping in enumerate(config.mailboxes):
            if job.cancelling:
                break

            target_user = mapping.target_user
            job.mailbox = target_user
            job.message = f"Repairing {target_user}"
            job.progress_pct = (mi / total_mb) * 100
            broadcaster.broadcast({"event": "repair_start", "mailbox": target_user})

            mb_cfg = SourceConfig(
                host=config.source.host, port=config.source.port, ssl=config.source.ssl,
                username=mapping.source_user, password=mapping.source_password,
            )

            with IMAPSource(mb_cfg) as imap:
                folders = imap.list_folders()
                for fi, folder_name in enumerate(folders):
                    if job.cancelling:
                        break

                    with state._cursor() as cur:
                        cur.execute(
                            "SELECT uid, graph_msg_id FROM messages "
                            "WHERE mailbox = ? AND folder = ? AND status = ? AND graph_msg_id IS NOT NULL",
                            (mapping.source_user, folder_name, "success"),
                        )
                        records = [(r["uid"], r["graph_msg_id"]) for r in cur.fetchall()]

                    if not records:
                        continue

                    try:
                        imap.select_folder(folder_name)
                    except Exception:
                        continue

                    repaired = 0
                    for uid, graph_msg_id in records:
                        if job.cancelling:
                            break
                        try:
                            msg = imap.fetch_message(uid)
                            if msg is None:
                                continue
                            is_read = "\\Seen" in msg.flags
                            mapi_flags = 0x01 if is_read else 0x00
                            extended_props = [{"id": "Integer 0x0E07", "value": str(mapi_flags)}]
                            original_date = _parse_imap_date(msg.internal_date)
                            if original_date:
                                extended_props.append({"id": "SystemTime 0x0E06", "value": original_date})
                                extended_props.append({"id": "SystemTime 0x0039", "value": original_date})
                            patch = {"isRead": is_read, "singleValueExtendedProperties": extended_props}
                            if "\\Flagged" in msg.flags:
                                patch["importance"] = "high"
                            graph.update_message(target_user, graph_msg_id, patch)
                            repaired += 1
                        except Exception:
                            pass

                    broadcaster.broadcast({
                        "event": "repair_progress",
                        "mailbox": target_user,
                        "folder": folder_name,
                        "repaired": repaired,
                        "total": len(records),
                    })

        state.close()
        job.progress_pct = 100
        job.message = "Repair complete"
        broadcaster.broadcast({"event": "complete", "status": "repair_done"})
    except Exception as exc:
        log.error("Repair failed: %s", exc, exc_info=True)
        job.error = str(exc)[:500]
        broadcaster.broadcast({"event": "error", "message": str(exc)[:500]})
    finally:
        job.running = False


# ---------------------------------------------------------------------------
# Merge folders
# ---------------------------------------------------------------------------

def start_merge(config: AppConfig) -> Job:
    return _launch("merge", _run_merge, (config,))


def _run_merge(job: Job, config: AppConfig) -> None:
    try:
        _ensure_ws_handler()
        setup_logging(level=config.options.log_level, log_file=config.options.log_file)
        graph = GraphClient(
            config.target,
            max_retries=config.options.max_retries,
            request_delay=config.options.request_delay,
        )

        MERGE_MAP = {
            "sent": "sentitems", "sent messages": "sentitems",
            "sent mail": "sentitems", "gesendet": "sentitems",
            "drafts": "drafts", "entwürfe": "drafts",
            "trash": "deleteditems", "papierkorb": "deleteditems",
            "junk": "junkemail", "spam": "junkemail",
            "archive": "archive", "archiv": "archive",
        }

        total_mb = len(config.mailboxes)
        for mi, mapping in enumerate(config.mailboxes):
            if job.cancelling:
                break

            target_user = mapping.target_user
            job.mailbox = target_user
            job.message = f"Merging folders for {target_user}"
            job.progress_pct = (mi / total_mb) * 100
            broadcaster.broadcast({"event": "merge_start", "mailbox": target_user})

            all_folders = graph.list_mail_folders(target_user)

            well_known_ids: dict[str, str] = {}
            for wk_name in set(MERGE_MAP.values()):
                try:
                    resp = graph.get(f"/users/{target_user}/mailFolders/{wk_name}?$select=id,displayName")
                    well_known_ids[wk_name] = resp.json()["id"]
                except Exception:
                    pass

            for folder in all_folders:
                if job.cancelling:
                    break
                display_name = folder.get("displayName", "")
                folder_id = folder.get("id", "")
                key = display_name.lower().strip()
                if key not in MERGE_MAP:
                    continue
                target_wk = MERGE_MAP[key]
                if target_wk not in well_known_ids:
                    continue
                target_folder_id = well_known_ids[target_wk]
                if folder_id == target_folder_id:
                    continue

                moved = 0
                while True:
                    try:
                        resp = graph.get(
                            f"/users/{target_user}/mailFolders/{folder_id}/messages?$select=id&$top=50"
                        )
                        messages = resp.json().get("value", [])
                    except Exception:
                        break
                    if not messages:
                        break
                    for msg in messages:
                        try:
                            graph.post(
                                f"/users/{target_user}/messages/{msg['id']}/move",
                                json={"destinationId": target_folder_id},
                            )
                            moved += 1
                        except Exception:
                            pass

                broadcaster.broadcast({
                    "event": "merge_progress",
                    "mailbox": target_user,
                    "folder": display_name,
                    "moved": moved,
                })

                if moved > 0:
                    try:
                        graph._request("DELETE", f"/users/{target_user}/mailFolders/{folder_id}")
                    except Exception:
                        pass

        job.progress_pct = 100
        job.message = "Merge complete"
        broadcaster.broadcast({"event": "complete", "status": "merge_done"})
    except Exception as exc:
        log.error("Merge failed: %s", exc, exc_info=True)
        job.error = str(exc)[:500]
        broadcaster.broadcast({"event": "error", "message": str(exc)[:500]})
    finally:
        job.running = False


# ---------------------------------------------------------------------------
# Dry-run
# ---------------------------------------------------------------------------

def start_dryrun(config: AppConfig) -> Job:
    return _launch("dryrun", _run_dryrun, (config,))


def _run_dryrun(job: Job, config: AppConfig) -> None:
    try:
        _ensure_ws_handler()
        results: list[dict] = []

        total_mb = len(config.mailboxes)
        for i, mapping in enumerate(config.mailboxes):
            if job.cancelling:
                break

            job.mailbox = mapping.source_user
            job.message = f"Testing {mapping.source_user} ({i+1}/{total_mb})"
            job.progress_pct = (i / total_mb) * 100

            result: dict[str, Any] = {
                "source_user": mapping.source_user,
                "target_user": mapping.target_user,
                "imap_ok": False,
                "imap_folders": 0,
                "graph_ok": False,
            }

            # Test IMAP
            mb_cfg = SourceConfig(
                host=config.source.host, port=config.source.port, ssl=config.source.ssl,
                username=mapping.source_user, password=mapping.source_password,
            )
            try:
                with IMAPSource(mb_cfg) as imap:
                    folders = imap.list_folders()
                    result["imap_ok"] = True
                    result["imap_folders"] = len(folders)
            except Exception as exc:
                result["imap_error"] = str(exc)[:200]

            # Test Graph API
            try:
                graph = GraphClient(config.target, max_retries=1)
                ok = graph.validate_user(mapping.target_user)
                result["graph_ok"] = ok
                if not ok:
                    result["graph_error"] = "User not found in M365"
            except Exception as exc:
                result["graph_error"] = str(exc)[:200]

            results.append(result)
            broadcaster.broadcast({"event": "dryrun_result", "result": result})

        job.progress_pct = 100
        job.stats = {"results": results}
        job.message = "Dry-run complete"
        broadcaster.broadcast({"event": "complete", "status": "dryrun_done", "results": results})
    except Exception as exc:
        job.error = str(exc)[:500]
        broadcaster.broadcast({"event": "error", "message": str(exc)[:500]})
    finally:
        job.running = False


# ---------------------------------------------------------------------------
# Relocate (delete wrongly-placed messages, clear state)
# ---------------------------------------------------------------------------

def start_fix_drafts(config: AppConfig) -> Job:
    return _launch("fix_drafts", _run_fix_drafts, (config,))


def _run_fix_drafts(job: Job, config: AppConfig) -> None:
    """Fix drafts by re-creating messages as non-drafts via Graph API."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from src.graph_client import GraphAPIError

    try:
        _ensure_ws_handler()
        setup_logging(level=config.options.log_level, log_file=config.options.log_file)

        workers = max(config.options.max_workers, 4)
        delay = min(config.options.request_delay, 0.3)

        graph = GraphClient(
            config.target,
            max_retries=config.options.max_retries,
            request_delay=delay,
        )

        # Deduplicate target mailboxes
        seen: set[str] = set()
        targets: list[str] = []
        for m in config.mailboxes:
            if m.target_user not in seen:
                seen.add(m.target_user)
                targets.append(m.target_user)

        grand_fixed = 0
        grand_failed = 0

        for ti, target_user in enumerate(targets):
            if job.cancelling:
                break

            job.mailbox = target_user
            job.message = f"Querying drafts in {target_user}..."
            job.progress_pct = (ti / len(targets)) * 100
            broadcaster.broadcast({
                "event": "fix_drafts_start",
                "mailbox": target_user,
            })

            try:
                drafts = graph.fetch_draft_messages(target_user)
            except Exception as exc:
                log.error("Failed to query drafts for %s: %s", target_user, exc)
                continue

            if not drafts:
                log.info("%s: no drafts found", target_user)
                continue

            log.info("%s: %d draft messages to re-create", target_user, len(drafts))
            job.message = f"Re-creating {len(drafts)} drafts in {target_user}"

            fixed = 0
            failed = 0

            def _recreate_one(msg: dict) -> bool:
                try:
                    graph.recreate_as_non_draft(target_user, msg)
                    return True
                except GraphAPIError as exc:
                    if exc.status_code == 404:
                        return True
                    return False
                except Exception:
                    return False

            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(_recreate_one, m): m for m in drafts}
                done_count = 0
                for future in as_completed(futures):
                    if job.cancelling:
                        pool.shutdown(wait=False, cancel_futures=True)
                        break
                    ok = future.result()
                    if ok:
                        fixed += 1
                    else:
                        failed += 1
                    done_count += 1
                    if done_count % 25 == 0:
                        pct_mailbox = done_count / len(drafts)
                        pct_overall = ((ti + pct_mailbox) / len(targets)) * 100
                        job.progress_pct = pct_overall
                        job.message = f"Re-created {fixed}/{len(drafts)} in {target_user}"
                        broadcaster.broadcast({
                            "event": "progress",
                            "mailbox": target_user,
                            "fixed": fixed,
                            "total": len(drafts),
                        })

            grand_fixed += fixed
            grand_failed += failed
            broadcaster.broadcast({
                "event": "fix_drafts_mailbox_done",
                "mailbox": target_user,
                "fixed": fixed,
                "failed": failed,
            })

        job.progress_pct = 100
        job.message = f"Fix-drafts complete: {grand_fixed} fixed, {grand_failed} failed"
        job.stats = {"fixed": grand_fixed, "failed": grand_failed}
        broadcaster.broadcast({
            "event": "complete",
            "status": "fix_drafts_done",
            "fixed": grand_fixed,
            "failed": grand_failed,
        })
    except Exception as exc:
        log.error("Fix-drafts failed: %s", exc, exc_info=True)
        job.error = str(exc)[:500]
        broadcaster.broadcast({"event": "error", "message": str(exc)[:500]})
    finally:
        job.running = False


def start_clean(config: AppConfig) -> Job:
    return _launch("clean", _run_clean, (config,))


def _run_clean(job: Job, config: AppConfig) -> None:
    """Delete all draft (isDraft=true) messages from target mailboxes."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from src.graph_client import GraphAPIError

    try:
        _ensure_ws_handler()
        setup_logging(level=config.options.log_level, log_file=config.options.log_file)

        graph = GraphClient(
            config.target,
            max_retries=config.options.max_retries,
            request_delay=min(config.options.request_delay, 0.2),
        )
        workers = max(config.options.max_workers, 8)

        seen: set[str] = set()
        targets: list[str] = []
        for m in config.mailboxes:
            if m.target_user not in seen:
                seen.add(m.target_user)
                targets.append(m.target_user)

        grand_deleted = 0
        grand_failed = 0

        for ti, target in enumerate(targets):
            if job.cancelling:
                break

            job.mailbox = target
            job.message = f"Querying drafts in {target}..."
            job.progress_pct = (ti / len(targets)) * 100

            try:
                drafts = graph.fetch_draft_messages(target)
            except Exception as exc:
                log.error("Failed to query drafts for %s: %s", target, exc)
                continue

            if not drafts:
                continue

            log.info("%s: %d drafts to delete", target, len(drafts))
            job.message = f"Deleting {len(drafts)} drafts from {target}"
            deleted = 0
            failed = 0

            def _del(msg_id: str) -> bool:
                try:
                    graph._request("DELETE", f"/users/{target}/messages/{msg_id}")
                    return True
                except GraphAPIError as exc:
                    return exc.status_code == 404
                except Exception:
                    return False

            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(_del, d["id"]): d for d in drafts}
                done = 0
                for f in as_completed(futures):
                    if job.cancelling:
                        break
                    if f.result():
                        deleted += 1
                    else:
                        failed += 1
                    done += 1
                    if done % 50 == 0:
                        pct = ((ti + done / len(drafts)) / len(targets)) * 100
                        job.progress_pct = pct
                        job.message = f"Deleted {deleted}/{len(drafts)} from {target}"

            grand_deleted += deleted
            grand_failed += failed
            broadcaster.broadcast({
                "event": "clean_mailbox_done",
                "mailbox": target,
                "deleted": deleted,
                "failed": failed,
            })

        job.progress_pct = 100
        job.message = f"Clean complete: {grand_deleted} deleted, {grand_failed} failed"
        job.stats = {"deleted": grand_deleted, "failed": grand_failed}
        broadcaster.broadcast({
            "event": "complete",
            "status": "clean_done",
            "deleted": grand_deleted,
            "failed": grand_failed,
        })
    except Exception as exc:
        log.error("Clean failed: %s", exc, exc_info=True)
        job.error = str(exc)[:500]
        broadcaster.broadcast({"event": "error", "message": str(exc)[:500]})
    finally:
        job.running = False


def start_purge(config: AppConfig) -> Job:
    return _launch("purge", _run_purge, (config,))


def _run_purge(job: Job, config: AppConfig) -> None:
    try:
        _ensure_ws_handler()
        setup_logging(level=config.options.log_level, log_file=config.options.log_file)

        from src.graph_client import GraphAPIError

        graph = GraphClient(
            config.target,
            max_retries=config.options.max_retries,
            request_delay=config.options.request_delay,
        )

        purgeable = [m for m in config.mailboxes if m.old_target_user]
        if not purgeable:
            job.message = "No mailboxes have old_target_user set"
            broadcaster.broadcast({"event": "error", "message": job.message})
            return

        grand_deleted = 0
        grand_failed = 0

        for mi, mapping in enumerate(purgeable):
            if job.cancelling:
                break

            source_user = mapping.source_user
            old_target = mapping.old_target_user

            job.mailbox = source_user
            job.message = f"Purge: fetching IMAP Message-IDs for {source_user}"
            job.progress_pct = (mi / len(purgeable)) * 100

            broadcaster.broadcast({
                "event": "purge_start",
                "source": source_user,
                "old_target": old_target,
            })

            # Step 1: Get Message-IDs from IMAP
            mb_cfg = SourceConfig(
                host=config.source.host, port=config.source.port, ssl=config.source.ssl,
                username=source_user, password=mapping.source_password,
            )
            try:
                with IMAPSource(mb_cfg) as imap:
                    imap_ids = imap.fetch_all_message_ids()
            except Exception as exc:
                log.error("IMAP failed for %s: %s", source_user, exc)
                continue

            if not imap_ids:
                continue

            # Step 2: Get M365 messages
            job.message = f"Purge: fetching M365 messages from {old_target}"
            try:
                m365_map = graph.fetch_all_message_ids(old_target)
            except Exception as exc:
                log.error("Graph API failed for %s: %s", old_target, exc)
                continue

            # Step 3: Match
            matches = [(mid, m365_map[mid]) for mid in imap_ids if mid in m365_map]
            log.info("%s: %d IMAP IDs, %d M365 IDs, %d matches", source_user, len(imap_ids), len(m365_map), len(matches))

            if not matches:
                continue

            # Step 4: Delete
            job.message = f"Purge: deleting {len(matches)} messages from {old_target}"
            deleted = 0
            failed = 0
            for mid, gid in matches:
                if job.cancelling:
                    break
                try:
                    graph._request("DELETE", f"/users/{old_target}/messages/{gid}")
                    deleted += 1
                except GraphAPIError as exc:
                    if exc.status_code == 404:
                        deleted += 1
                    else:
                        failed += 1
                except Exception:
                    failed += 1

            grand_deleted += deleted
            grand_failed += failed
            broadcaster.broadcast({
                "event": "purge_progress",
                "source": source_user,
                "deleted": deleted,
                "failed": failed,
                "matches": len(matches),
            })

        job.progress_pct = 100
        job.message = f"Purge complete: {grand_deleted} deleted, {grand_failed} failed"
        job.stats = {"deleted": grand_deleted, "failed": grand_failed}
        broadcaster.broadcast({
            "event": "complete",
            "status": "purge_done",
            "deleted": grand_deleted,
            "failed": grand_failed,
        })
    except Exception as exc:
        log.error("Purge failed: %s", exc, exc_info=True)
        job.error = str(exc)[:500]
        broadcaster.broadcast({"event": "error", "message": str(exc)[:500]})
    finally:
        job.running = False


def start_relocate(config: AppConfig) -> Job:
    return _launch("relocate", _run_relocate, (config,))


def _run_relocate(job: Job, config: AppConfig) -> None:
    try:
        _ensure_ws_handler()
        setup_logging(level=config.options.log_level, log_file=config.options.log_file)

        from src.graph_client import GraphAPIError

        state = StateDB(config.options.state_db)
        graph = GraphClient(
            config.target,
            max_retries=config.options.max_retries,
            request_delay=config.options.request_delay,
        )

        relocatable = [m for m in config.mailboxes if m.old_target_user]
        if not relocatable:
            job.message = "No mailboxes have old_target_user set"
            broadcaster.broadcast({"event": "error", "message": job.message})
            return

        total_deleted = 0
        total_failed = 0

        for mi, mapping in enumerate(relocatable):
            if job.cancelling:
                break

            source_user = mapping.source_user
            old_target = mapping.old_target_user

            job.mailbox = source_user
            job.message = f"Relocating {source_user}: deleting from {old_target}"
            job.progress_pct = (mi / len(relocatable)) * 100

            broadcaster.broadcast({
                "event": "relocate_start",
                "source": source_user,
                "old_target": old_target,
                "new_target": mapping.target_user,
            })

            with state._cursor() as cur:
                cur.execute(
                    "SELECT uid, folder, graph_msg_id FROM messages "
                    "WHERE mailbox = ? AND status = ? AND graph_msg_id IS NOT NULL",
                    (source_user, "success"),
                )
                records = [(r["uid"], r["folder"], r["graph_msg_id"]) for r in cur.fetchall()]

            if not records:
                log.info("No records for %s — skipping", source_user)
                continue

            log.info("Relocating %d messages for %s (deleting from %s)", len(records), source_user, old_target)

            deleted = 0
            failed = 0
            not_found = 0

            for uid, folder, graph_msg_id in records:
                if job.cancelling:
                    break
                try:
                    graph._request("DELETE", f"/users/{old_target}/messages/{graph_msg_id}")
                    deleted += 1
                except GraphAPIError as exc:
                    if exc.status_code == 404:
                        not_found += 1
                    else:
                        failed += 1
                except Exception:
                    failed += 1

            total_deleted += deleted
            total_failed += failed

            # Clear state DB records
            with state._cursor() as cur:
                cur.execute("DELETE FROM messages WHERE mailbox = ?", (source_user,))
                cleared = cur.rowcount
            with state._cursor() as cur:
                cur.execute("DELETE FROM folders WHERE mailbox = ?", (source_user,))

            broadcaster.broadcast({
                "event": "relocate_progress",
                "source": source_user,
                "deleted": deleted,
                "not_found": not_found,
                "failed": failed,
                "cleared": cleared,
            })
            log.info(
                "%s: deleted %d, not_found %d, failed %d, cleared %d state records",
                source_user, deleted, not_found, failed, cleared,
            )

        state.close()
        job.progress_pct = 100
        job.message = f"Relocate complete: {total_deleted} deleted, {total_failed} failed"
        job.stats = {"deleted": total_deleted, "failed": total_failed}
        broadcaster.broadcast({
            "event": "complete",
            "status": "relocate_done",
            "deleted": total_deleted,
            "failed": total_failed,
        })
    except Exception as exc:
        log.error("Relocate failed: %s", exc, exc_info=True)
        job.error = str(exc)[:500]
        broadcaster.broadcast({"event": "error", "message": str(exc)[:500]})
    finally:
        job.running = False
