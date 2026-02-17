"""SQLite state database for migration resume / dedup tracking."""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Generator, Optional

from .logger import get_logger

log = get_logger("state")


class MigrationStatus(str, Enum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class MessageRecord:
    """A single migrated-message record."""
    mailbox: str
    folder: str
    uid: int
    status: MigrationStatus
    graph_message_id: Optional[str] = None
    error: Optional[str] = None
    migrated_at: Optional[str] = None


class StateDB:
    """Thin wrapper around an SQLite database that tracks migration progress."""

    def __init__(self, db_path: str | Path = "migration_state.db") -> None:
        self._path = str(db_path)
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = threading.Lock()
        self._ensure_schema()

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self._path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    @contextmanager
    def _cursor(self) -> Generator[sqlite3.Cursor, None, None]:
        with self._lock:
            conn = self._connect()
            cur = conn.cursor()
            try:
                yield cur
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _ensure_schema(self) -> None:
        with self._cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    mailbox      TEXT    NOT NULL,
                    folder       TEXT    NOT NULL,
                    uid          INTEGER NOT NULL,
                    status       TEXT    NOT NULL DEFAULT 'pending',
                    graph_msg_id TEXT,
                    error        TEXT,
                    migrated_at  TEXT,
                    PRIMARY KEY (mailbox, folder, uid)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS folders (
                    mailbox        TEXT NOT NULL,
                    imap_folder    TEXT NOT NULL,
                    graph_folder_id TEXT,
                    created_at     TEXT,
                    PRIMARY KEY (mailbox, imap_folder)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS runs (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    ended_at   TEXT,
                    status     TEXT NOT NULL DEFAULT 'running'
                )
            """)
        log.debug("State DB schema verified at %s", self._path)

    # ------------------------------------------------------------------
    # Run tracking
    # ------------------------------------------------------------------

    def start_run(self) -> int:
        with self._cursor() as cur:
            cur.execute(
                "INSERT INTO runs (started_at, status) VALUES (?, ?)",
                (datetime.now(timezone.utc).isoformat(), "running"),
            )
            run_id = cur.lastrowid
        log.info("Migration run #%d started", run_id)
        return run_id

    def end_run(self, run_id: int, status: str = "completed") -> None:
        with self._cursor() as cur:
            cur.execute(
                "UPDATE runs SET ended_at = ?, status = ? WHERE id = ?",
                (datetime.now(timezone.utc).isoformat(), status, run_id),
            )

    # ------------------------------------------------------------------
    # Folder tracking
    # ------------------------------------------------------------------

    def upsert_folder(self, mailbox: str, imap_folder: str, graph_folder_id: str) -> None:
        with self._cursor() as cur:
            cur.execute(
                """INSERT INTO folders (mailbox, imap_folder, graph_folder_id, created_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(mailbox, imap_folder) DO UPDATE SET graph_folder_id = excluded.graph_folder_id""",
                (mailbox, imap_folder, graph_folder_id, datetime.now(timezone.utc).isoformat()),
            )

    def get_graph_folder_id(self, mailbox: str, imap_folder: str) -> Optional[str]:
        with self._cursor() as cur:
            cur.execute(
                "SELECT graph_folder_id FROM folders WHERE mailbox = ? AND imap_folder = ?",
                (mailbox, imap_folder),
            )
            row = cur.fetchone()
            return row["graph_folder_id"] if row else None

    # ------------------------------------------------------------------
    # Message tracking
    # ------------------------------------------------------------------

    def is_migrated(self, mailbox: str, folder: str, uid: int) -> bool:
        """Return True if the message has already been successfully migrated."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT 1 FROM messages WHERE mailbox = ? AND folder = ? AND uid = ? AND status = ?",
                (mailbox, folder, uid, MigrationStatus.SUCCESS.value),
            )
            return cur.fetchone() is not None

    def get_migrated_uids(self, mailbox: str, folder: str) -> set[int]:
        """Return the set of UIDs already successfully migrated for a folder."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT uid FROM messages WHERE mailbox = ? AND folder = ? AND status = ?",
                (mailbox, folder, MigrationStatus.SUCCESS.value),
            )
            return {row["uid"] for row in cur.fetchall()}

    def record_message(
        self,
        mailbox: str,
        folder: str,
        uid: int,
        status: MigrationStatus,
        graph_message_id: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        with self._cursor() as cur:
            cur.execute(
                """INSERT INTO messages (mailbox, folder, uid, status, graph_msg_id, error, migrated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(mailbox, folder, uid) DO UPDATE SET
                       status       = excluded.status,
                       graph_msg_id = excluded.graph_msg_id,
                       error        = excluded.error,
                       migrated_at  = excluded.migrated_at""",
                (
                    mailbox, folder, uid,
                    status.value,
                    graph_message_id,
                    error,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_stats(self, mailbox: Optional[str] = None) -> dict[str, int]:
        """Return counts by status, optionally filtered by mailbox."""
        with self._cursor() as cur:
            if mailbox:
                cur.execute(
                    "SELECT status, COUNT(*) as cnt FROM messages WHERE mailbox = ? GROUP BY status",
                    (mailbox,),
                )
            else:
                cur.execute("SELECT status, COUNT(*) as cnt FROM messages GROUP BY status")
            return {row["status"]: row["cnt"] for row in cur.fetchall()}

    def get_folder_stats(self, mailbox: str, folder: str) -> dict[str, int]:
        with self._cursor() as cur:
            cur.execute(
                "SELECT status, COUNT(*) as cnt FROM messages WHERE mailbox = ? AND folder = ? GROUP BY status",
                (mailbox, folder),
            )
            return {row["status"]: row["cnt"] for row in cur.fetchall()}

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
