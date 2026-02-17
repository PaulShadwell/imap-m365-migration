#!/usr/bin/env python3
"""IMAP to Microsoft 365 Migration Tool — main entry point.

Usage:
    python migrate.py                      # uses config.yaml in current dir
    python migrate.py --config my.yaml     # custom config path
    python migrate.py --dry-run            # validate config without migrating
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src.config import ConfigError, load_config
from src.graph_client import GraphClient
from src.logger import setup_logging, get_logger
from src.mail_migrator import MailMigrator, MigrationStats, FolderStats
from src.state import StateDB

console = Console()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate mailboxes from an IMAP server to Microsoft 365 Exchange Online.",
    )
    parser.add_argument(
        "--config", "-c",
        default="config.yaml",
        help="Path to the YAML configuration file (default: config.yaml)",
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Validate configuration and IMAP connectivity without migrating.",
    )
    parser.add_argument(
        "--fix-drafts",
        action="store_true",
        help="FAST draft fix — queries M365 for draft messages and clears the "
             "draft flag via Graph API using concurrent workers. No IMAP needed.",
    )
    parser.add_argument(
        "--repair",
        action="store_true",
        help="Fix already-migrated messages: clear draft flag, restore original dates.",
    )
    parser.add_argument(
        "--merge-folders",
        action="store_true",
        help="Merge duplicate folders (e.g. 'Sent' into 'Sent Items') and delete the empties.",
    )
    parser.add_argument(
        "--relocate",
        action="store_true",
        help="Delete wrongly-placed messages from old_target_user mailboxes and clear "
             "state DB so they can be re-migrated to the correct target_user.",
    )
    parser.add_argument(
        "--purge",
        action="store_true",
        help="Thorough cleanup: match IMAP Message-IDs against the old_target M365 "
             "mailbox and delete any matches. Catches messages missed by --relocate.",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete all draft (isDraft=true) messages from target mailboxes. "
             "Preserves any real emails received since the migration started.",
    )
    parser.add_argument(
        "--mailbox", action="append", metavar="SOURCE_EMAIL",
        help="Only process this source mailbox (repeatable). "
             "If omitted, all mailboxes in config are processed.",
    )
    return parser.parse_args()


def _filter_mailboxes(
    cfg: "AppConfig", args: argparse.Namespace
) -> list["MailboxMapping"]:
    """Return only the mailboxes selected by ``--mailbox``, or all if unset."""
    if not args.mailbox:
        return cfg.mailboxes
    selected = {m.lower() for m in args.mailbox}
    filtered = [m for m in cfg.mailboxes if m.source_user.lower() in selected]
    if not filtered:
        console.print(
            f"[red]No mailboxes matched --mailbox {args.mailbox}. "
            f"Available: {[m.source_user for m in cfg.mailboxes]}[/red]"
        )
    return filtered


def print_banner() -> None:
    console.print(Panel.fit(
        "[bold]IMAP  →  Microsoft 365  Migration Tool[/bold]",
        border_style="blue",
    ))


def print_summary(all_stats: list[tuple[str, MigrationStats]], elapsed: float) -> None:
    """Print a summary table of results with per-folder detail."""
    # ── Main summary table ──
    table = Table(title="Migration Summary", show_lines=True)
    table.add_column("Mailbox", style="bold")
    table.add_column("Folders", justify="right")
    table.add_column("Total Msgs", justify="right")
    table.add_column("Migrated", justify="right", style="green")
    table.add_column("Skipped", justify="right", style="yellow")
    table.add_column("Failed", justify="right", style="red")
    table.add_column("Success %", justify="right")

    total_migrated = 0
    total_failed = 0

    for label, stats in all_stats:
        table.add_row(
            label,
            str(stats.folders_processed),
            str(stats.messages_total),
            str(stats.messages_migrated),
            str(stats.messages_skipped),
            str(stats.messages_failed),
            f"{stats.success_rate:.1f}%",
        )
        total_migrated += stats.messages_migrated
        total_failed += stats.messages_failed

    console.print()
    console.print(table)

    # ── Per-folder detail tables (one per mailbox) ──
    for label, stats in all_stats:
        if not stats.folder_stats:
            continue
        ftable = Table(
            title=f"Folder Detail: {label}",
            show_lines=False,
            padding=(0, 1),
        )
        ftable.add_column("Folder", style="cyan")
        ftable.add_column("Source", justify="right")
        ftable.add_column("Migrated", justify="right", style="green")
        ftable.add_column("Skipped", justify="right", style="yellow")
        ftable.add_column("Failed", justify="right", style="red")

        folder_total_src = 0
        folder_total_mig = 0
        folder_total_skip = 0
        folder_total_fail = 0

        for fs in sorted(stats.folder_stats, key=lambda f: f.name):
            ftable.add_row(
                fs.name,
                str(fs.source_count),
                str(fs.migrated),
                str(fs.skipped),
                str(fs.failed),
            )
            folder_total_src += fs.source_count
            folder_total_mig += fs.migrated
            folder_total_skip += fs.skipped
            folder_total_fail += fs.failed

        ftable.add_row(
            "[bold]TOTAL[/bold]",
            f"[bold]{folder_total_src}[/bold]",
            f"[bold green]{folder_total_mig}[/bold green]",
            f"[bold yellow]{folder_total_skip}[/bold yellow]",
            f"[bold red]{folder_total_fail}[/bold red]",
        )
        console.print()
        console.print(ftable)

    console.print(f"\n[dim]Elapsed: {elapsed:.1f}s[/dim]")

    if total_failed > 0:
        console.print(
            f"\n[bold red]WARNING:[/bold red] {total_failed} message(s) failed. "
            "Check the log file for details and re-run to retry."
        )


def dry_run(args: argparse.Namespace) -> None:
    """Validate config and test IMAP connectivity without migrating."""
    log = get_logger()
    cfg = load_config(args.config)
    console.print("[green]Configuration loaded and validated successfully.[/green]")
    console.print(f"  Source:    {cfg.source.host}:{cfg.source.port}")
    console.print(f"  Target:    tenant {cfg.target.tenant_id[:8]}...")
    mailboxes = _filter_mailboxes(cfg, args)
    console.print(f"  Mailboxes: {len(mailboxes)}")

    # Test IMAP connection for each mailbox
    from src.config import SourceConfig
    from src.imap_source import IMAPSource
    console.print("\nTesting IMAP connections...")
    for mb in mailboxes:
        console.print(f"\n  Mailbox: [bold]{mb.source_user}[/bold]")
        mb_cfg = SourceConfig(
            host=cfg.source.host,
            port=cfg.source.port,
            ssl=cfg.source.ssl,
            username=mb.source_user,
            password=mb.source_password,
        )
        try:
            with IMAPSource(mb_cfg) as imap:
                folders = imap.list_folders()
                console.print(f"  [green]Connected. Found {len(folders)} folders.[/green]")
                for f in folders[:15]:
                    console.print(f"    {f}")
                if len(folders) > 15:
                    console.print(f"    ... and {len(folders) - 15} more")
        except Exception as exc:
            console.print(f"  [red]IMAP connection failed: {exc}[/red]")

    # Test Graph API connection
    console.print("\nTesting Microsoft Graph API connection...")
    try:
        graph = GraphClient(cfg.target, max_retries=1)
        for mb in mailboxes:
            ok = graph.validate_user(mb.target_user)
            status = "[green]OK[/green]" if ok else "[red]NOT FOUND[/red]"
            console.print(f"  {mb.target_user}: {status}")
    except Exception as exc:
        console.print(f"[red]Graph API connection failed: {exc}[/red]")
        sys.exit(1)

    console.print("\n[green bold]Dry run complete — everything looks good.[/green bold]")


def fix_drafts(args: argparse.Namespace) -> None:
    """Fix draft-flagged messages by re-creating them as non-drafts.

    Exchange Online ignores PATCH updates to ``PR_MESSAGE_FLAGS`` on
    existing messages — ``isDraft`` never changes.  The only working
    approach is to **delete** each draft and **re-create** it via a
    JSON ``POST`` that includes ``singleValueExtendedProperties`` with
    ``Integer 0x0E07`` in the creation payload.

    For each draft the tool:

    1. GETs the full message content (body, headers, from, to, etc.)
    2. GETs attachments
    3. POSTs a new message with ``singleValueExtendedProperties``
    4. Uploads any large attachments
    5. Moves to the original folder
    6. Deletes the original draft

    A verification step on the first message confirms ``isDraft=false``
    before processing the rest.  No IMAP needed.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from src.graph_client import GraphAPIError

    cfg = load_config(args.config)
    log = setup_logging(level=cfg.options.log_level, log_file=cfg.options.log_file)

    # Each message requires 4-6 API calls, so use moderate concurrency
    # to avoid 429 throttling while still being fast.
    workers = max(cfg.options.max_workers, 4)
    delay = min(cfg.options.request_delay, 0.3)

    graph = GraphClient(
        cfg.target,
        max_retries=cfg.options.max_retries,
        request_delay=delay,
    )

    console.print(
        "[bold]Fix-drafts mode:[/bold] re-creating draft messages as non-drafts\n"
        f"  Workers: {workers}  Delay: {delay}s\n"
    )

    grand_fixed = 0
    grand_failed = 0

    # Collect unique target mailboxes (filtered by --mailbox if set)
    mailboxes = _filter_mailboxes(cfg, args)
    seen_targets: set[str] = set()
    targets: list[str] = []
    for mapping in mailboxes:
        t = mapping.target_user
        if t not in seen_targets:
            seen_targets.add(t)
            targets.append(t)

    for target_user in targets:
        console.print(f"[bold]{target_user}[/bold]")
        console.print("  Querying for draft messages...")

        try:
            drafts = graph.fetch_draft_messages(target_user)
        except Exception as exc:
            console.print(f"  [red]Failed to query drafts: {exc}[/red]\n")
            continue

        if not drafts:
            console.print("  [green]No drafts found — clean![/green]\n")
            continue

        console.print(f"  [yellow]{len(drafts)} draft messages to fix[/yellow]")

        # ── Verify on the first message ──────────────────────────────
        first = drafts[0]
        console.print("  Verifying re-creation on first message...")
        try:
            new_id = graph.recreate_as_non_draft(target_user, first)
            ok, details = graph.verify_non_draft(target_user, new_id)
            if ok:
                subj = details.get("subject", "")[:50]
                from_addr = (details.get("from", {}) or {}).get(
                    "emailAddress", {}
                ).get("address", "?")
                console.print(
                    f"  [green]Verified! isDraft=false[/green]\n"
                    f"    Subject: {subj}\n"
                    f"    From:    {from_addr}"
                )
            else:
                console.print(
                    "  [red bold]FAILED: isDraft still true after re-creation.[/red bold]\n"
                    "  Cannot fix drafts with this approach. Aborting.\n"
                )
                return
        except Exception as exc:
            console.print(f"  [red]Verification error: {exc}[/red]\n  Aborting.\n")
            return

        fixed = 1  # first message already done
        failed = 0

        from rich.progress import (
            Progress, SpinnerColumn, TextColumn, BarColumn,
            MofNCompleteColumn, TimeRemainingColumn,
        )

        def _recreate_one(msg: dict) -> bool:
            try:
                graph.recreate_as_non_draft(target_user, msg)
                return True
            except GraphAPIError as exc:
                if exc.status_code == 404:
                    return True
                log.warning("Failed to recreate %s: %s",
                            msg["id"][:30], exc)
                return False
            except Exception as exc:
                log.warning("Recreate error: %s", exc)
                return False

        remaining = drafts[1:]
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
            transient=False,
        ) as progress:
            task = progress.add_task(
                f"  Re-creating {target_user}", total=len(drafts)
            )
            progress.update(task, advance=1)  # first already done

            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {
                    pool.submit(_recreate_one, msg): msg
                    for msg in remaining
                }
                for future in as_completed(futures):
                    ok = future.result()
                    if ok:
                        fixed += 1
                    else:
                        failed += 1
                    progress.update(task, advance=1)

        console.print(
            f"  Fixed: [green]{fixed}[/green]  Failed: [red]{failed}[/red]\n"
        )
        grand_fixed += fixed
        grand_failed += failed

    console.print(
        f"\n[bold]Summary:[/bold]\n"
        f"  Mailboxes processed: {len(targets)}\n"
        f"  Messages re-created: [green]{grand_fixed}[/green]\n"
        f"  Failures: [red]{grand_failed}[/red]\n"
    )

    if grand_fixed > 0:
        console.print(
            "[green bold]Done! Emails should no longer appear as drafts.[/green bold]\n"
        )


def repair(args: argparse.Namespace) -> None:
    """Fix already-migrated messages: clear draft flag and restore original dates.

    Iterates over all successfully-migrated messages in the state DB, connects
    to the source IMAP to read the original date, then PATCHes each message
    in Exchange Online via extended MAPI properties.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from src.config import SourceConfig
    from src.imap_source import IMAPSource

    cfg = load_config(args.config)
    log = setup_logging(level=cfg.options.log_level, log_file=cfg.options.log_file)
    state = StateDB(cfg.options.state_db)
    graph = GraphClient(cfg.target, max_retries=cfg.options.max_retries, request_delay=cfg.options.request_delay)

    console.print("[bold]Repair mode:[/bold] clearing draft flags and restoring dates on migrated messages.\n")

    for mapping in _filter_mailboxes(cfg, args):
        target_user = mapping.target_user
        console.print(f"[bold]Repairing mailbox:[/bold] {target_user}")

        # Connect to IMAP to get original dates
        mb_cfg = SourceConfig(
            host=cfg.source.host, port=cfg.source.port, ssl=cfg.source.ssl,
            username=mapping.source_user, password=mapping.source_password,
        )

        with IMAPSource(mb_cfg) as imap:
            folders = imap.list_folders()

            for folder_name in folders:
                # Get successfully-migrated message IDs for this folder
                migrated_uids = state.get_migrated_uids(mapping.source_user, folder_name)
                if not migrated_uids:
                    continue

                # Get the Graph message IDs from the state DB
                records = _get_migrated_records(state, mapping.source_user, folder_name)
                if not records:
                    continue

                console.print(f"  {folder_name}: {len(records)} messages to repair")

                # Select the IMAP folder to fetch dates
                try:
                    imap.select_folder(folder_name)
                except Exception as exc:
                    log.warning("  Cannot select '%s': %s", folder_name, exc)
                    continue

                repaired = 0
                failed = 0

                def repair_one(uid: int, graph_msg_id: str) -> bool:
                    """Repair a single message."""
                    # Fetch just the flags and internal date from IMAP
                    try:
                        msg = imap.fetch_message(uid)
                    except Exception:
                        return False

                    if msg is None:
                        return False

                    is_read = "\\Seen" in msg.flags
                    mapi_flags = 0x01 if is_read else 0x00

                    extended_props = [
                        {"id": "Integer 0x0E07", "value": str(mapi_flags)},
                    ]

                    # Parse the original date
                    from src.mail_migrator import _parse_imap_date
                    original_date = _parse_imap_date(msg.internal_date)
                    if original_date:
                        extended_props.append({"id": "SystemTime 0x0E06", "value": original_date})
                        extended_props.append({"id": "SystemTime 0x0039", "value": original_date})

                    patch = {
                        "isRead": is_read,
                        "singleValueExtendedProperties": extended_props,
                    }
                    if "\\Flagged" in msg.flags:
                        patch["importance"] = "high"

                    try:
                        graph.update_message(target_user, graph_msg_id, patch)
                        return True
                    except Exception as exc:
                        log.debug("  Repair failed for UID %d: %s", uid, exc)
                        return False

                # IMAP is not thread-safe, so repair sequentially
                from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn
                with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
                              BarColumn(), MofNCompleteColumn(), transient=False) as progress:
                    task = progress.add_task(f"    {folder_name}", total=len(records))
                    for uid, graph_msg_id in records:
                        ok = repair_one(uid, graph_msg_id)
                        if ok:
                            repaired += 1
                        else:
                            failed += 1
                        progress.update(task, advance=1)

                log.info("  %s: %d repaired, %d failed", folder_name, repaired, failed)

    state.close()
    console.print("\n[green bold]Repair complete.[/green bold]")


def merge_folders(args: argparse.Namespace) -> None:
    """Merge duplicate/misplaced folders into their correct well-known Exchange folders.

    Finds custom folders whose names match well-known folders (e.g. 'Sent',
    'Sent Messages'), moves all their messages into the proper Exchange
    well-known folder (e.g. 'Sent Items'), then deletes the now-empty custom
    folder.
    """
    from src.graph_client import GraphAPIError

    cfg = load_config(args.config)
    log = setup_logging(level=cfg.options.log_level, log_file=cfg.options.log_file)
    graph = GraphClient(cfg.target, max_retries=cfg.options.max_retries, request_delay=cfg.options.request_delay)

    # Map of display names (lowercased) to their well-known folder identifier.
    # These are custom folders that should have been mapped to well-known ones.
    MERGE_MAP = {
        "sent": "sentitems",
        "sent messages": "sentitems",
        "sent mail": "sentitems",
        "gesendet": "sentitems",
        "drafts": "drafts",
        "entwürfe": "drafts",
        "trash": "deleteditems",
        "papierkorb": "deleteditems",
        "junk": "junkemail",
        "spam": "junkemail",
        "archive": "archive",
        "archiv": "archive",
    }

    console.print("[bold]Merge folders mode:[/bold] moving messages from duplicate folders into well-known folders.\n")

    for mapping in _filter_mailboxes(cfg, args):
        target_user = mapping.target_user
        console.print(f"[bold]Processing mailbox:[/bold] {target_user}")

        # Get all folders for this user
        all_folders = graph.list_mail_folders(target_user)

        # Index well-known folder IDs
        well_known_ids: dict[str, str] = {}
        for wk_name in set(MERGE_MAP.values()):
            try:
                resp = graph.get(f"/users/{target_user}/mailFolders/{wk_name}?$select=id,displayName")
                wk = resp.json()
                well_known_ids[wk_name] = wk["id"]
                log.debug("Well-known '%s' -> id=%s (%s)", wk_name, wk["id"], wk.get("displayName"))
            except Exception:
                log.warning("  Could not find well-known folder '%s'", wk_name)

        # Find custom folders that should be merged
        for folder in all_folders:
            display_name = folder.get("displayName", "")
            folder_id = folder.get("id", "")
            key = display_name.lower().strip()

            if key not in MERGE_MAP:
                continue

            target_wk = MERGE_MAP[key]
            if target_wk not in well_known_ids:
                continue

            target_folder_id = well_known_ids[target_wk]

            # Don't merge a well-known folder into itself
            if folder_id == target_folder_id:
                continue

            console.print(f"  [yellow]Found duplicate:[/yellow] '{display_name}' → merging into well-known '{target_wk}'")

            # Move all messages from the custom folder to the well-known folder
            moved = 0
            failed = 0
            while True:
                # Fetch a batch of message IDs from the custom folder
                try:
                    resp = graph.get(
                        f"/users/{target_user}/mailFolders/{folder_id}/messages"
                        f"?$select=id&$top=50"
                    )
                    messages = resp.json().get("value", [])
                except Exception as exc:
                    log.error("  Error listing messages: %s", exc)
                    break

                if not messages:
                    break  # folder is empty

                for msg in messages:
                    msg_id = msg["id"]
                    try:
                        graph.post(
                            f"/users/{target_user}/messages/{msg_id}/move",
                            json={"destinationId": target_folder_id},
                        )
                        moved += 1
                    except Exception as exc:
                        log.warning("  Failed to move message %s: %s", msg_id[:20], exc)
                        failed += 1

                log.info("  Moved batch: %d so far, %d failed", moved, failed)

            console.print(f"    Moved {moved} messages ({failed} failed)")

            # Delete the now-empty custom folder
            if failed == 0:
                try:
                    graph._request("DELETE", f"/users/{target_user}/mailFolders/{folder_id}")
                    console.print(f"    [green]Deleted empty folder '{display_name}'[/green]")
                except Exception as exc:
                    log.warning("  Could not delete folder '%s': %s", display_name, exc)
                    console.print(f"    [yellow]Could not delete folder '{display_name}': {exc}[/yellow]")
            else:
                console.print(f"    [yellow]Folder not deleted (has {failed} unmoved messages)[/yellow]")

    console.print("\n[green bold]Merge complete.[/green bold]")


def relocate(args: argparse.Namespace) -> None:
    """Delete wrongly-placed messages and clear state so they can be re-migrated.

    For each mailbox mapping that has ``old_target_user`` set, this command:
    1. Finds all successfully-migrated graph_msg_ids for that source in the state DB.
    2. Deletes each message from the **old** target mailbox via Graph API.
    3. Removes the state DB records so the messages are eligible for re-migration.

    After running this, update ``target_user`` to the correct (shared) mailbox
    and re-run ``python migrate.py`` to migrate them to the right place.
    """
    from src.graph_client import GraphAPIError

    cfg = load_config(args.config)
    log = setup_logging(level=cfg.options.log_level, log_file=cfg.options.log_file)
    state = StateDB(cfg.options.state_db)
    graph = GraphClient(
        cfg.target,
        max_retries=cfg.options.max_retries,
        request_delay=cfg.options.request_delay,
    )

    # Only process mailboxes that have old_target_user defined
    relocatable = [m for m in _filter_mailboxes(cfg, args) if m.old_target_user]
    if not relocatable:
        console.print(
            "[yellow]No mailbox mappings have 'old_target_user' set.[/yellow]\n"
            "Add 'old_target_user' to each mapping that needs relocation in config.yaml.\n"
            "Example:\n"
            "  - source_user: info@jakobsimon.com\n"
            "    target_user: info@jakobsimon.com      # new shared mailbox\n"
            "    old_target_user: info@simon-immobilien.ch  # where messages are now\n"
        )
        state.close()
        return

    console.print("[bold]Relocate mode:[/bold] deleting wrongly-placed messages so they can be re-migrated.\n")

    total_deleted = 0
    total_failed = 0
    total_cleared = 0

    for mapping in relocatable:
        source_user = mapping.source_user
        old_target = mapping.old_target_user
        new_target = mapping.target_user

        console.print(
            f"[bold]{source_user}[/bold]\n"
            f"  Deleting from: [red]{old_target}[/red]\n"
            f"  Will re-migrate to: [green]{new_target}[/green]"
        )

        # Get all successfully-migrated records for this source from the state DB
        with state._cursor() as cur:
            cur.execute(
                "SELECT uid, folder, graph_msg_id FROM messages "
                "WHERE mailbox = ? AND status = ? AND graph_msg_id IS NOT NULL",
                (source_user, "success"),
            )
            records = [(r["uid"], r["folder"], r["graph_msg_id"]) for r in cur.fetchall()]

        if not records:
            console.print("  [dim]No migrated messages found in state DB — skipping.[/dim]\n")
            continue

        console.print(f"  Found {len(records)} messages to relocate")

        # Delete each message from the old target mailbox
        deleted = 0
        failed = 0
        not_found = 0

        from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn
        with Progress(
            SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
            BarColumn(), MofNCompleteColumn(), transient=False,
        ) as progress:
            task = progress.add_task(f"  Deleting from {old_target}", total=len(records))

            for uid, folder, graph_msg_id in records:
                try:
                    graph._request(
                        "DELETE",
                        f"/users/{old_target}/messages/{graph_msg_id}",
                    )
                    deleted += 1
                except GraphAPIError as exc:
                    if exc.status_code == 404:
                        not_found += 1
                    else:
                        log.debug(
                            "  Failed to delete UID %d (folder=%s) from %s: %s",
                            uid, folder, old_target, exc,
                        )
                        failed += 1
                except Exception as exc:
                    log.debug("  Delete error UID %d: %s", uid, exc)
                    failed += 1
                progress.update(task, advance=1)

        console.print(
            f"  Deleted: [green]{deleted}[/green]  "
            f"Not found (already gone): [yellow]{not_found}[/yellow]  "
            f"Failed: [red]{failed}[/red]"
        )
        total_deleted += deleted
        total_failed += failed

        # Clear state DB records so these messages can be re-migrated
        with state._cursor() as cur:
            cur.execute(
                "DELETE FROM messages WHERE mailbox = ?",
                (source_user,),
            )
            cleared = cur.rowcount
            total_cleared += cleared

        # Also clear folder mappings for this source
        with state._cursor() as cur:
            cur.execute(
                "DELETE FROM folders WHERE mailbox = ?",
                (source_user,),
            )

        console.print(f"  Cleared {cleared} state DB records\n")

    state.close()

    console.print(
        f"\n[bold]Summary:[/bold]\n"
        f"  Messages deleted from old mailboxes: [green]{total_deleted}[/green]\n"
        f"  Failures: [red]{total_failed}[/red]\n"
        f"  State DB records cleared: {total_cleared}\n"
    )

    if total_cleared > 0:
        console.print(
            "[bold green]Next steps:[/bold green]\n"
            "  1. Create the shared mailboxes in M365 if not already done.\n"
            "  2. Verify 'target_user' in config.yaml points to the shared mailboxes.\n"
            "  3. Remove 'old_target_user' from config.yaml (no longer needed).\n"
            "  4. Run: [bold]python migrate.py[/bold]  — to re-migrate to the correct mailboxes.\n"
            "  5. Run: [bold]python migrate.py --repair[/bold]  — to fix draft flags and dates.\n"
        )


def clean(args: argparse.Namespace) -> None:
    """Delete all draft messages from target mailboxes.

    Uses ``$filter=isDraft eq true`` so only migrated-but-broken messages
    are removed.  Any real emails received since the migration started
    are untouched because normal incoming mail is never a draft.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from src.graph_client import GraphAPIError

    cfg = load_config(args.config)
    log = setup_logging(level=cfg.options.log_level, log_file=cfg.options.log_file)
    graph = GraphClient(
        cfg.target,
        max_retries=cfg.options.max_retries,
        request_delay=cfg.options.request_delay,
    )
    workers = min(cfg.options.max_workers, 4)

    # Collect unique target mailboxes (filtered by --mailbox if set)
    mailboxes = _filter_mailboxes(cfg, args)
    seen: set[str] = set()
    targets: list[str] = []
    for m in mailboxes:
        t = m.target_user
        if t not in seen:
            seen.add(t)
            targets.append(t)

    console.print("[bold]Clean mode:[/bold] deleting draft (migrated) messages.\n")

    # Count drafts per mailbox first
    total_drafts = 0
    mailbox_counts: list[tuple[str, int]] = []
    for target in targets:
        try:
            drafts = graph.fetch_draft_messages(target)
            mailbox_counts.append((target, len(drafts)))
            total_drafts += len(drafts)
            console.print(f"  {target}: [yellow]{len(drafts)}[/yellow] drafts")
        except Exception as exc:
            console.print(f"  {target}: [red]error — {exc}[/red]")
            mailbox_counts.append((target, 0))

    if total_drafts == 0:
        console.print("\n[green]No draft messages found — nothing to clean.[/green]")
        return

    console.print(f"\n  Total: [bold yellow]{total_drafts}[/bold yellow] draft messages to delete.\n")

    # Confirmation
    answer = console.input(
        "[bold red]Delete these draft messages? This cannot be undone.[/bold red]\n"
        "Type [bold]YES[/bold] to confirm: "
    )
    if answer.strip() != "YES":
        console.print("Aborted.")
        return

    console.print()

    grand_deleted = 0
    grand_failed = 0

    for target, count in mailbox_counts:
        if count == 0:
            continue

        console.print(f"[bold]{target}[/bold]: deleting {count} drafts")

        # Re-fetch to get IDs (the earlier fetch already has them)
        try:
            drafts = graph.fetch_draft_messages(target)
        except Exception as exc:
            console.print(f"  [red]Failed: {exc}[/red]\n")
            continue

        deleted = 0
        failed = 0

        from rich.progress import (
            Progress, SpinnerColumn, TextColumn, BarColumn,
            MofNCompleteColumn, TimeRemainingColumn,
        )

        def _delete_one(msg_id: str) -> bool:
            try:
                graph._request("DELETE", f"/users/{target}/messages/{msg_id}")
                return True
            except GraphAPIError as exc:
                if exc.status_code == 404:
                    return True
                return False
            except Exception:
                return False

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(), MofNCompleteColumn(), TimeRemainingColumn(),
            transient=False,
        ) as progress:
            task = progress.add_task(f"  Cleaning {target}", total=len(drafts))
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {
                    pool.submit(_delete_one, d["id"]): d for d in drafts
                }
                for future in as_completed(futures):
                    if future.result():
                        deleted += 1
                    else:
                        failed += 1
                    progress.update(task, advance=1)

        console.print(
            f"  Deleted: [green]{deleted}[/green]  Failed: [red]{failed}[/red]\n"
        )
        grand_deleted += deleted
        grand_failed += failed

    console.print(
        f"\n[bold]Clean Summary:[/bold]\n"
        f"  Deleted: [green]{grand_deleted}[/green]\n"
        f"  Failed: [red]{grand_failed}[/red]\n\n"
        "[bold green]Next steps:[/bold green]\n"
        "  1. Delete migration_state.db:  [bold]rm migration_state.db[/bold]\n"
        "  2. Re-migrate:  [bold]python migrate.py[/bold]\n"
    )


def purge(args: argparse.Namespace) -> None:
    """Thorough cleanup: match IMAP Message-IDs against M365 and delete matches.

    Unlike ``--relocate`` (which uses state DB graph_msg_ids that may be stale),
    this command:

    1. Connects to each jakobsimon.com IMAP source and fetches every
       Message-ID header across all folders.
    2. Fetches every message's ``internetMessageId`` from the old target
       M365 mailbox.
    3. Computes the intersection — messages present in both.
    4. Deletes those messages from the M365 mailbox.

    This catches messages whose Graph IDs changed (e.g. after --merge-folders
    or --repair) that --relocate missed.
    """
    from src.config import SourceConfig
    from src.imap_source import IMAPSource
    from src.graph_client import GraphAPIError

    cfg = load_config(args.config)
    log = setup_logging(level=cfg.options.log_level, log_file=cfg.options.log_file)
    graph = GraphClient(
        cfg.target,
        max_retries=cfg.options.max_retries,
        request_delay=cfg.options.request_delay,
    )

    purgeable = [m for m in _filter_mailboxes(cfg, args) if m.old_target_user]
    if not purgeable:
        console.print(
            "[yellow]No mailbox mappings have 'old_target_user' set.[/yellow]\n"
            "Add 'old_target_user' to each mapping that needs purging."
        )
        return

    console.print(
        "[bold]Purge mode:[/bold] matching IMAP Message-IDs against M365 for precise deletion.\n"
    )

    grand_total_deleted = 0
    grand_total_failed = 0

    for mapping in purgeable:
        source_user = mapping.source_user
        old_target = mapping.old_target_user
        new_target = mapping.target_user

        console.print(
            f"[bold]{source_user}[/bold]\n"
            f"  IMAP source → collecting Message-IDs..."
        )

        # Step 1: Get all Message-IDs from the IMAP source
        mb_cfg = SourceConfig(
            host=cfg.source.host, port=cfg.source.port, ssl=cfg.source.ssl,
            username=source_user, password=mapping.source_password,
        )
        try:
            with IMAPSource(mb_cfg) as imap:
                imap_message_ids = imap.fetch_all_message_ids()
        except Exception as exc:
            console.print(f"  [red]IMAP connection failed: {exc}[/red]\n")
            continue

        if not imap_message_ids:
            console.print("  [dim]No messages in IMAP source — skipping.[/dim]\n")
            continue

        console.print(f"  Found {len(imap_message_ids)} unique Message-IDs in IMAP")

        # Step 2: Get all messages from the old M365 target mailbox
        console.print(f"  Fetching messages from M365 mailbox [bold]{old_target}[/bold]...")
        try:
            m365_id_map = graph.fetch_all_message_ids(old_target)
        except Exception as exc:
            console.print(f"  [red]Graph API failed: {exc}[/red]\n")
            continue

        console.print(f"  Found {len(m365_id_map)} messages in M365 mailbox")

        # Step 3: Compute intersection
        matches: list[tuple[str, str]] = []  # (internetMessageId, graphId)
        for mid in imap_message_ids:
            if mid in m365_id_map:
                matches.append((mid, m365_id_map[mid]))

        if not matches:
            console.print("  [green]No wrongly-placed messages found — clean![/green]\n")
            continue

        console.print(f"  [yellow]{len(matches)} messages to delete from {old_target}[/yellow]")

        # Step 4: Delete matches
        deleted = 0
        failed = 0

        from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn
        with Progress(
            SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
            BarColumn(), MofNCompleteColumn(), transient=False,
        ) as progress:
            task = progress.add_task(f"  Purging from {old_target}", total=len(matches))

            for mid, graph_id in matches:
                try:
                    graph._request("DELETE", f"/users/{old_target}/messages/{graph_id}")
                    deleted += 1
                except GraphAPIError as exc:
                    if exc.status_code == 404:
                        deleted += 1  # already gone, count as success
                    else:
                        log.debug("  Failed to delete %s: %s", mid[:40], exc)
                        failed += 1
                except Exception as exc:
                    log.debug("  Delete error: %s", exc)
                    failed += 1
                progress.update(task, advance=1)

        console.print(
            f"  Deleted: [green]{deleted}[/green]  Failed: [red]{failed}[/red]\n"
        )
        grand_total_deleted += deleted
        grand_total_failed += failed

    console.print(
        f"\n[bold]Purge Summary:[/bold]\n"
        f"  Total deleted: [green]{grand_total_deleted}[/green]\n"
        f"  Total failed: [red]{grand_total_failed}[/red]\n"
    )

    if grand_total_deleted > 0:
        console.print(
            "[bold green]Next steps:[/bold green]\n"
            "  1. Remove 'old_target_user' from config.yaml.\n"
            "  2. Run: [bold]python migrate.py[/bold]  — to migrate to the shared mailboxes.\n"
            "  3. Run: [bold]python migrate.py --repair[/bold]  — to fix draft flags.\n"
        )


def _get_migrated_records(state: StateDB, mailbox: str, folder: str) -> list[tuple]:
    """Return (uid, graph_msg_id) pairs for successfully-migrated messages."""
    with state._cursor() as cur:
        cur.execute(
            "SELECT uid, graph_msg_id FROM messages WHERE mailbox = ? AND folder = ? AND status = ? AND graph_msg_id IS NOT NULL",
            (mailbox, folder, "success"),
        )
        return [(row["uid"], row["graph_msg_id"]) for row in cur.fetchall()]


def main() -> None:
    args = parse_args()
    print_banner()

    # Load configuration
    try:
        cfg = load_config(args.config)
    except ConfigError as exc:
        console.print(f"[red]{exc}[/red]")
        sys.exit(1)
    except Exception as exc:
        console.print(f"[red]Failed to load config: {exc}[/red]")
        sys.exit(1)

    # Set up logging
    log = setup_logging(level=cfg.options.log_level, log_file=cfg.options.log_file)

    # Dry-run mode
    if args.dry_run:
        dry_run(args)
        return

    # Fast draft fix (Graph API only, concurrent)
    if args.fix_drafts:
        fix_drafts(args)
        return

    # Repair mode (full: drafts + dates, requires IMAP)
    if args.repair:
        repair(args)
        return

    # Merge duplicate folders
    if args.merge_folders:
        merge_folders(args)
        return

    # Relocate wrongly-placed messages
    if args.relocate:
        relocate(args)
        return

    # Purge by Message-ID matching (thorough cleanup)
    if args.purge:
        purge(args)
        return

    # Clean draft messages from target mailboxes
    if args.clean:
        clean(args)
        return

    # Full migration
    mailboxes = _filter_mailboxes(cfg, args)
    log.info("Configuration loaded from %s", args.config)
    log.info("Source: %s:%d", cfg.source.host, cfg.source.port)
    log.info("Mailboxes to migrate: %d", len(mailboxes))

    # Initialize components
    state = StateDB(cfg.options.state_db)
    graph = GraphClient(cfg.target, max_retries=cfg.options.max_retries, request_delay=cfg.options.request_delay)
    migrator = MailMigrator(cfg.source, graph, state, cfg.options)

    run_id = state.start_run()
    all_stats: list[tuple[str, MigrationStats]] = []
    start_time = time.time()
    exit_status = "completed"

    try:
        for mapping in mailboxes:
            label = f"{mapping.source_user} → {mapping.target_user}"
            try:
                stats = migrator.migrate_mailbox(mapping)
                all_stats.append((label, stats))
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                log.error("Mailbox '%s' failed: %s", label, exc, exc_info=True)
                failed_stats = MigrationStats()
                failed_stats.errors.append(str(exc))
                all_stats.append((label, failed_stats))

    except KeyboardInterrupt:
        console.print("\n[yellow]Migration interrupted by user.[/yellow]")
        exit_status = "interrupted"
    except Exception as exc:
        log.error("Fatal error: %s", exc, exc_info=True)
        exit_status = "error"
    finally:
        elapsed = time.time() - start_time
        state.end_run(run_id, exit_status)
        state.close()

    # Print summary
    print_summary(all_stats, elapsed)

    # Also dump overall state DB stats
    overall = state if exit_status != "error" else None
    log.info("Run #%d finished with status: %s (%.1fs)", run_id, exit_status, elapsed)

    if exit_status != "completed":
        sys.exit(1)


if __name__ == "__main__":
    main()
