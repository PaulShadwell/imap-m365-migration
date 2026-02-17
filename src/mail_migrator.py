"""Mail migration orchestrator — ties IMAP source, Graph client, and state together."""

from __future__ import annotations

import email.utils
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from .config import MailboxMapping, Options, SourceConfig
from .folder_mapper import FolderMapper
from .graph_client import GraphClient, GraphAPIError
from .imap_source import IMAPSource, IMAPMessage
from .logger import get_logger
from .state import MigrationStatus, StateDB

log = get_logger("migrator")


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

@dataclass
class FolderStats:
    """Counters for a single folder within a mailbox migration."""
    name: str = ""
    source_count: int = 0
    migrated: int = 0
    skipped: int = 0
    failed: int = 0


@dataclass
class MigrationStats:
    """Counters for a single mailbox migration."""
    folders_processed: int = 0
    messages_total: int = 0
    messages_migrated: int = 0
    messages_skipped: int = 0  # already migrated in a prior run
    messages_failed: int = 0
    errors: list[str] = field(default_factory=list)
    folder_stats: list[FolderStats] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        attempted = self.messages_migrated + self.messages_failed
        if attempted == 0:
            return 100.0
        return (self.messages_migrated / attempted) * 100


# ---------------------------------------------------------------------------
# Migrator
# ---------------------------------------------------------------------------

class MailMigrator:
    """Migrate one or more mailboxes from IMAP to Exchange Online."""

    def __init__(
        self,
        source_config: SourceConfig,
        graph: GraphClient,
        state: StateDB,
        options: Options,
    ) -> None:
        self._src_cfg = source_config
        self._graph = graph
        self._state = state
        self._opts = options
        self._verified_non_draft = False  # set after first-message check

    def migrate_mailbox(self, mapping: MailboxMapping) -> MigrationStats:
        """Run the full migration for a single mailbox mapping."""
        stats = MigrationStats()
        src_user = mapping.source_user
        tgt_user = mapping.target_user
        log.info("=" * 60)
        log.info("Starting migration: %s -> %s", src_user, tgt_user)
        log.info("=" * 60)

        # Validate target user exists in M365
        if not self._graph.validate_user(tgt_user):
            msg = f"Target user '{tgt_user}' not found in Microsoft 365"
            log.error(msg)
            stats.errors.append(msg)
            return stats

        # Connect to IMAP as the source user (each mailbox has its own password)
        src_cfg = SourceConfig(
            host=self._src_cfg.host,
            port=self._src_cfg.port,
            ssl=self._src_cfg.ssl,
            username=mapping.source_user,
            password=mapping.source_password,
        )

        with IMAPSource(src_cfg) as imap:
            # List source folders
            all_folders = imap.list_folders()
            folders = self._filter_folders(all_folders, mapping)
            log.info("Will migrate %d folders (of %d total)", len(folders), len(all_folders))

            # Create folder mapper
            mapper = FolderMapper(self._graph, self._state, tgt_user)

            # Process each folder
            with self._make_progress() as progress:
                folder_task = progress.add_task(
                    "[bold blue]Folders", total=len(folders)
                )

                for folder_name in folders:
                    self._migrate_folder(
                        imap, mapper, mapping, folder_name, stats, progress, folder_task
                    )
                    stats.folders_processed += 1
                    progress.update(folder_task, advance=1)

        log.info("-" * 60)
        log.info("Migration complete for %s -> %s", src_user, tgt_user)
        log.info(
            "  Folders: %d | Messages: %d total, %d migrated, %d skipped, %d failed",
            stats.folders_processed,
            stats.messages_total,
            stats.messages_migrated,
            stats.messages_skipped,
            stats.messages_failed,
        )
        if stats.messages_migrated + stats.messages_failed > 0:
            log.info("  Success rate: %.1f%%", stats.success_rate)
        return stats

    # ------------------------------------------------------------------
    # Per-folder migration
    # ------------------------------------------------------------------

    def _migrate_folder(
        self,
        imap: IMAPSource,
        mapper: FolderMapper,
        mapping: MailboxMapping,
        folder_name: str,
        stats: MigrationStats,
        progress: Progress,
        folder_task: int,
    ) -> None:
        mailbox_key = mapping.source_user
        log.info("Processing folder: %s", folder_name)

        # Get all UIDs in the source folder
        try:
            uids = imap.fetch_uids(folder_name)
        except Exception as exc:
            msg = f"Cannot list messages in '{folder_name}': {exc}"
            log.error(msg)
            stats.errors.append(msg)
            return

        # Track per-folder stats
        fs = FolderStats(name=folder_name, source_count=len(uids))
        stats.folder_stats.append(fs)

        if not uids:
            log.info("  Folder '%s' is empty — skipping", folder_name)
            return

        stats.messages_total += len(uids)

        # Determine which UIDs are already migrated
        migrated_uids = self._state.get_migrated_uids(mailbox_key, folder_name)
        pending_uids = [u for u in uids if u not in migrated_uids]
        skip_count = len(uids) - len(pending_uids)
        stats.messages_skipped += skip_count
        fs.skipped = skip_count

        if skip_count > 0:
            log.info(
                "  Folder '%s': %d messages, %d already migrated, %d pending",
                folder_name, len(uids), skip_count, len(pending_uids),
            )

        if not pending_uids:
            fs.migrated = 0
            fs.failed = 0
            return

        # Resolve the target Exchange folder (creates it if needed)
        try:
            graph_folder_id = mapper.resolve(folder_name, mailbox_key)
        except Exception as exc:
            msg = f"Cannot resolve target folder for '{folder_name}': {exc}"
            log.error(msg)
            stats.errors.append(msg)
            stats.messages_failed += len(pending_uids)
            return

        # Migrate messages in batches
        msg_task = progress.add_task(
            f"  [cyan]{folder_name}", total=len(pending_uids)
        )

        migrated_before = stats.messages_migrated
        failed_before = stats.messages_failed

        batch_size = self._opts.batch_size
        for i in range(0, len(pending_uids), batch_size):
            batch_uids = pending_uids[i : i + batch_size]
            self._migrate_batch(
                imap, mapping, folder_name, graph_folder_id,
                batch_uids, stats, progress, msg_task,
            )

        fs.migrated = stats.messages_migrated - migrated_before
        fs.failed = stats.messages_failed - failed_before

        progress.remove_task(msg_task)

    # ------------------------------------------------------------------
    # Per-batch migration
    # ------------------------------------------------------------------

    def _migrate_batch(
        self,
        imap: IMAPSource,
        mapping: MailboxMapping,
        folder_name: str,
        graph_folder_id: str,
        uids: list[int],
        stats: MigrationStats,
        progress: Progress,
        task_id: int,
    ) -> None:
        mailbox_key = mapping.source_user
        target_user = mapping.target_user
        max_workers = self._opts.max_workers

        # Phase 1: Fetch messages from IMAP (sequential — single connection).
        messages = imap.fetch_messages_batch(uids)

        # Phase 2: Upload to Graph API in parallel.
        if max_workers <= 1:
            # Sequential fallback
            for msg in messages:
                ok = self._upload_one(msg, target_user, graph_folder_id, mailbox_key, folder_name, stats)
                progress.update(task_id, advance=1)
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {
                    pool.submit(
                        self._upload_one, msg, target_user, graph_folder_id,
                        mailbox_key, folder_name, stats,
                    ): msg
                    for msg in messages
                }
                for future in as_completed(futures):
                    # Exception handling is inside _upload_one; result is unused.
                    future.result()
                    progress.update(task_id, advance=1)

    # ------------------------------------------------------------------
    # Single message upload (thread-safe)
    # ------------------------------------------------------------------

    def _upload_one(
        self,
        msg: IMAPMessage,
        target_user: str,
        graph_folder_id: str,
        mailbox_key: str,
        folder_name: str,
        stats: MigrationStats,
    ) -> bool:
        """Upload one message. Returns True on success, False on failure.

        IMAP flags and dates are passed directly to ``upload_mime_message``
        which includes them as ``singleValueExtendedProperties`` in the
        JSON POST payload.  This ensures messages are created as
        **non-drafts** with correct dates — no separate PATCH needed.

        Thread-safe: only touches the thread-safe GraphClient and StateDB.
        Stats counters use simple increments (acceptable minor race on totals).
        """
        try:
            log.debug("  Migrating UID %d (%d bytes)", msg.uid, len(msg.raw))

            is_read = "\\Seen" in msg.flags
            is_flagged = "\\Flagged" in msg.flags
            imap_date_iso = _parse_imap_date(msg.internal_date)

            # Upload: parses MIME → JSON POST with extended properties
            # (non-draft from creation) → uploads attachments → moves
            result = self._graph.upload_mime_message(
                target_user,
                graph_folder_id,
                msg.raw,
                is_read=is_read,
                is_flagged=is_flagged,
                imap_date_iso=imap_date_iso,
            )
            graph_msg_id = result.get("id", "")

            # Verify first message is NOT a draft (safety check)
            if not self._verified_non_draft and graph_msg_id:
                self._verify_first_message(target_user, graph_msg_id, msg.uid)

            # Record success
            self._state.record_message(
                mailbox_key, folder_name, msg.uid,
                MigrationStatus.SUCCESS, graph_message_id=graph_msg_id,
            )
            stats.messages_migrated += 1
            log.debug("  UID %d -> %s", msg.uid, graph_msg_id)
            return True

        except Exception as exc:
            error_str = str(exc)[:500]
            log.warning("  UID %d failed: %s", msg.uid, error_str)
            self._state.record_message(
                mailbox_key, folder_name, msg.uid,
                MigrationStatus.FAILED, error=error_str,
            )
            stats.messages_failed += 1
            stats.errors.append(f"UID {msg.uid} in '{folder_name}': {error_str}")
            return False

    # ------------------------------------------------------------------
    # First-message verification
    # ------------------------------------------------------------------

    def _verify_first_message(
        self, target_user: str, graph_msg_id: str, uid: int
    ) -> None:
        """Check the first migrated message to confirm isDraft is False.

        If the message is still a draft, log a prominent warning so the
        operator knows something is wrong before the entire mailbox is
        processed.  This is a one-time check per migrator instance.
        """
        self._verified_non_draft = True
        try:
            resp = self._graph.get(
                f"/users/{target_user}/messages/{graph_msg_id}"
                "?$select=isDraft,receivedDateTime,subject"
            )
            data = resp.json()
            is_draft = data.get("isDraft", True)
            received = data.get("receivedDateTime", "?")
            subject = (data.get("subject") or "")[:60]

            if is_draft:
                log.warning(
                    "FIRST-MESSAGE CHECK FAILED: UID %d is still isDraft=true! "
                    "Subject: %s  ReceivedDate: %s. "
                    "The migration will continue, but all messages may be drafts.",
                    uid, subject, received,
                )
            else:
                log.info(
                    "First-message check PASSED: UID %d isDraft=false, "
                    "receivedDate=%s, subject=%s",
                    uid, received, subject,
                )
        except Exception as exc:
            log.warning("Could not verify first message UID %d: %s", uid, exc)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _filter_folders(
        self, all_folders: list[str], mapping: MailboxMapping
    ) -> list[str]:
        """Apply include/exclude filters from the mailbox mapping and global options."""
        folders = list(all_folders)

        if mapping.include_folders:
            include_set = {f.lower() for f in mapping.include_folders}
            folders = [f for f in folders if f.lower() in include_set]

        # Merge per-mailbox and global exclude lists
        excludes: list[str] = []
        if self._opts.exclude_folders:
            excludes.extend(self._opts.exclude_folders)
        if mapping.exclude_folders:
            excludes.extend(mapping.exclude_folders)
        if excludes:
            exclude_set = {f.lower() for f in excludes}
            folders = [f for f in folders if f.lower() not in exclude_set]

        return folders

    @staticmethod
    def _make_progress() -> Progress:
        return Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            transient=False,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_imap_date(internal_date: Optional[str]) -> Optional[str]:
    """Parse an IMAP INTERNALDATE string to ISO 8601 for Graph API.

    IMAP format: ``14-Feb-2026 10:30:00 +0100``
    Graph format: ``2026-02-14T09:30:00Z`` (UTC)
    """
    if not internal_date:
        return None
    try:
        # email.utils can parse RFC 2822 dates; IMAP dates are close enough
        dt = email.utils.parsedate_to_datetime(internal_date)
        # Convert to UTC
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        # Try common IMAP INTERNALDATE format directly
        try:
            dt = datetime.strptime(
                internal_date.strip('"'), "%d-%b-%Y %H:%M:%S %z"
            )
            dt_utc = dt.astimezone(timezone.utc)
            return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            return None
