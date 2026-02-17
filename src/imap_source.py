"""IMAP source — connect, list folders, fetch messages."""

from __future__ import annotations

import email.utils
import imaplib
import re
import ssl
from dataclasses import dataclass
from typing import Optional

from .config import SourceConfig
from .logger import get_logger

log = get_logger("imap")


@dataclass
class IMAPMessage:
    """Lightweight container for one fetched message."""
    uid: int
    raw: bytes                    # RFC 822 content
    flags: list[str]              # e.g. ["\\Seen", "\\Flagged"]
    internal_date: Optional[str]  # IMAP INTERNALDATE string


class IMAPSource:
    """Read-only facade over an IMAP mailbox."""

    def __init__(self, config: SourceConfig) -> None:
        self._cfg = config
        self._conn: Optional[imaplib.IMAP4 | imaplib.IMAP4_SSL] = None

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open the IMAP connection and authenticate."""
        host, port = self._cfg.host, self._cfg.port
        log.info("Connecting to IMAP server %s:%d (SSL=%s)", host, port, self._cfg.ssl)

        if self._cfg.ssl:
            ctx = ssl.create_default_context()
            self._conn = imaplib.IMAP4_SSL(host, port, ssl_context=ctx)
        else:
            self._conn = imaplib.IMAP4(host, port)

        self._conn.login(self._cfg.username, self._cfg.password)
        log.info("Authenticated as %s", self._cfg.username)

    def disconnect(self) -> None:
        if self._conn:
            try:
                self._conn.logout()
            except Exception:
                pass
            self._conn = None
            log.debug("IMAP connection closed")

    # ------------------------------------------------------------------
    # Folder operations
    # ------------------------------------------------------------------

    def list_folders(self) -> list[str]:
        """Return a list of all folder names (decoded from IMAP modified-UTF7)."""
        assert self._conn is not None, "Not connected"
        status, data = self._conn.list()
        if status != "OK":
            raise RuntimeError(f"IMAP LIST failed: {status}")

        folders: list[str] = []
        # Each item looks like: b'(\\HasNoChildren) "/" "INBOX"'
        pattern = re.compile(rb'\((?P<flags>[^)]*)\)\s+"(?P<sep>[^"]+)"\s+"?(?P<name>[^"]*)"?')
        for item in data:
            if item is None:
                continue
            raw = item if isinstance(item, bytes) else item[0] if isinstance(item, tuple) else b""
            m = pattern.match(raw)
            if m:
                name = m.group("name").decode("utf-7").replace("&", "+").rstrip()
                # Decode IMAP modified UTF-7 properly
                name = _decode_imap_utf7(m.group("name"))
                folders.append(name)
        log.info("Found %d folders", len(folders))
        log.debug("Folders: %s", folders)
        return folders

    def select_folder(self, folder: str) -> int:
        """Select a folder and return the number of messages in it."""
        assert self._conn is not None, "Not connected"
        status, data = self._conn.select(_encode_imap_utf7(folder), readonly=True)
        if status != "OK":
            raise RuntimeError(f"Cannot select folder '{folder}': {data}")
        count = int(data[0])
        log.debug("Selected folder '%s' — %d messages", folder, count)
        return count

    # ------------------------------------------------------------------
    # Message enumeration
    # ------------------------------------------------------------------

    def fetch_uids(self, folder: str) -> list[int]:
        """Return all message UIDs in the currently-selected *folder*."""
        self.select_folder(folder)
        assert self._conn is not None
        status, data = self._conn.uid("search", None, "ALL")
        if status != "OK":
            return []
        uid_bytes = data[0]
        if not uid_bytes or uid_bytes.strip() == b"":
            return []
        uids = [int(u) for u in uid_bytes.split()]
        log.debug("Folder '%s': %d UIDs", folder, len(uids))
        return uids

    # ------------------------------------------------------------------
    # Message fetch
    # ------------------------------------------------------------------

    def fetch_message(self, uid: int) -> Optional[IMAPMessage]:
        """Fetch a single message by UID (folder must already be selected)."""
        assert self._conn is not None
        # Fetch body, flags, and internal date in one round-trip.
        status, data = self._conn.uid(
            "fetch", str(uid), "(RFC822 FLAGS INTERNALDATE)"
        )
        if status != "OK" or not data or data[0] is None:
            log.warning("UID %d: fetch returned %s", uid, status)
            return None

        # data is a list of (envelope, body) tuples
        raw_response = data[0]
        if isinstance(raw_response, tuple):
            meta_line = raw_response[0]  # e.g. b'1 (UID 42 FLAGS (\\Seen) ...'
            body = raw_response[1]
        else:
            log.warning("UID %d: unexpected response format", uid)
            return None

        flags = _parse_flags(meta_line)
        internal_date = _parse_internal_date(meta_line)

        return IMAPMessage(uid=uid, raw=body, flags=flags, internal_date=internal_date)

    def fetch_message_ids(self, folder: str) -> set[str]:
        """Return the set of Message-ID headers for all messages in *folder*.

        This is a lightweight fetch that only retrieves the Message-ID header,
        making it much faster than fetching full message bodies.
        """
        self.select_folder(folder)
        assert self._conn is not None
        status, data = self._conn.uid("search", None, "ALL")
        if status != "OK" or not data or not data[0] or data[0].strip() == b"":
            return set()

        uids_str = data[0].decode()
        if not uids_str.strip():
            return set()

        # Fetch just the Message-ID header for all UIDs in one command
        status, data = self._conn.uid(
            "fetch", uids_str.replace(" ", ","),
            "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])"
        )
        if status != "OK":
            return set()

        message_ids: set[str] = set()
        for item in data:
            if item is None or isinstance(item, bytes):
                continue
            if isinstance(item, tuple) and len(item) >= 2:
                header = item[1]
                if isinstance(header, bytes):
                    header_str = header.decode("ascii", errors="replace").strip()
                    # Extract the Message-ID value from the header line
                    for line in header_str.split("\n"):
                        line = line.strip()
                        if line.lower().startswith("message-id:"):
                            mid = line.split(":", 1)[1].strip()
                            if mid:
                                message_ids.add(mid)
        log.debug("Folder '%s': %d Message-IDs fetched", folder, len(message_ids))
        return message_ids

    def fetch_all_message_ids(self) -> set[str]:
        """Return Message-ID headers across ALL folders."""
        all_ids: set[str] = set()
        folders = self.list_folders()
        for folder in folders:
            try:
                ids = self.fetch_message_ids(folder)
                all_ids.update(ids)
            except Exception as exc:
                log.warning("Could not fetch Message-IDs from '%s': %s", folder, exc)
        log.info("Total unique Message-IDs across all folders: %d", len(all_ids))
        return all_ids

    def fetch_messages_batch(self, uids: list[int]) -> list[IMAPMessage]:
        """Fetch multiple messages one-by-one (safe; avoids giant UID sets)."""
        results: list[IMAPMessage] = []
        for uid in uids:
            msg = self.fetch_message(uid)
            if msg:
                results.append(msg)
        return results

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "IMAPSource":
        self.connect()
        return self

    def __exit__(self, *exc) -> None:
        self.disconnect()


# ---------------------------------------------------------------------------
# IMAP modified-UTF7 helpers
# ---------------------------------------------------------------------------

def _decode_imap_utf7(raw: bytes) -> str:
    """Decode IMAP modified UTF-7 folder names to Python str."""
    # IMAP uses a variant of UTF-7 where '&' replaces '+' and ',' replaces '/'
    result: list[str] = []
    i = 0
    while i < len(raw):
        if raw[i:i+1] == b"&":
            end = raw.index(b"-", i + 1)
            if end == i + 1:
                # "&-" encodes a literal '&'
                result.append("&")
            else:
                encoded = b"+" + raw[i+1:end].replace(b",", b"/") + b"-"
                result.append(encoded.decode("utf-7"))
            i = end + 1
        else:
            result.append(chr(raw[i]))
            i += 1
    return "".join(result)


def _encode_imap_utf7(folder: str) -> str:
    """Encode a folder name to IMAP modified UTF-7 for SELECT/LIST commands."""
    # For pure-ASCII names this is a no-op (which covers the vast majority).
    try:
        folder.encode("ascii")
        return folder
    except UnicodeEncodeError:
        pass

    result: list[str] = []
    buf = ""
    for ch in folder:
        if ch == "&":
            if buf:
                encoded = buf.encode("utf-7").decode("ascii")
                result.append("&" + encoded[1:].replace("/", ","))
                buf = ""
            result.append("&-")
        elif ord(ch) < 0x80 and ch.isprintable():
            if buf:
                encoded = buf.encode("utf-7").decode("ascii")
                result.append("&" + encoded[1:].replace("/", ","))
                buf = ""
            result.append(ch)
        else:
            buf += ch
    if buf:
        encoded = buf.encode("utf-7").decode("ascii")
        result.append("&" + encoded[1:].replace("/", ","))
    return "".join(result)


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _parse_flags(meta: bytes) -> list[str]:
    """Extract FLAGS from an IMAP fetch response line."""
    m = re.search(rb"FLAGS\s*\(([^)]*)\)", meta, re.IGNORECASE)
    if not m:
        return []
    flags_str = m.group(1).decode("ascii", errors="replace").strip()
    if not flags_str:
        return []
    return flags_str.split()


def _parse_internal_date(meta: bytes) -> Optional[str]:
    """Extract INTERNALDATE from an IMAP fetch response line."""
    m = re.search(rb'INTERNALDATE\s+"([^"]+)"', meta, re.IGNORECASE)
    if m:
        return m.group(1).decode("ascii", errors="replace")
    return None
