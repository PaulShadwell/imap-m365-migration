"""Map IMAP folder names to Exchange Online mail folders."""

from __future__ import annotations

from typing import Optional

from .graph_client import GraphClient
from .logger import get_logger
from .state import StateDB

log = get_logger("folders")

# ---------------------------------------------------------------------------
# Well-known folder mapping
# ---------------------------------------------------------------------------
# IMAP folder names vary between providers.  This map covers the most common
# names and maps them to Exchange Online well-known folder names.
# Keys are lower-cased IMAP names; values are Exchange wellKnownName strings.
# See: https://learn.microsoft.com/en-us/graph/api/resources/mailfolder

_WELL_KNOWN: dict[str, str] = {
    # Inbox
    "inbox": "inbox",
    "posteingang": "inbox",
    # Sent
    "sent": "sentitems",
    "sent items": "sentitems",
    "sent messages": "sentitems",
    "sent mail": "sentitems",
    "gesendete elemente": "sentitems",
    "gesendete objekte": "sentitems",
    "gesendet": "sentitems",
    "[gmail]/sent mail": "sentitems",
    # Drafts
    "drafts": "drafts",
    "entwürfe": "drafts",
    "entw&APw-rfe": "drafts",  # IMAP modified UTF-7 encoding
    "[gmail]/drafts": "drafts",
    # Deleted / Trash
    "trash": "deleteditems",
    "deleted items": "deleteditems",
    "deleted messages": "deleteditems",
    "gelöschte elemente": "deleteditems",
    "gel&APY-schte elemente": "deleteditems",  # IMAP modified UTF-7
    "papierkorb": "deleteditems",
    "[gmail]/trash": "deleteditems",
    "[gmail]/bin": "deleteditems",
    # Junk / Spam
    "junk": "junkemail",
    "junk e-mail": "junkemail",
    "junk email": "junkemail",
    "spam": "junkemail",
    "bulk mail": "junkemail",
    "junk-e-mail": "junkemail",
    "[gmail]/spam": "junkemail",
    # Archive
    "archive": "archive",
    "archiv": "archive",
    "[gmail]/all mail": "archive",
}


class FolderMapper:
    """Resolves IMAP folder names to Graph API folder IDs in Exchange Online.

    - Standard folders (Inbox, Sent, etc.) are mapped to Exchange well-known
      folders so that messages end up in the expected place.
    - Custom / user-created folders are created on-demand in Exchange Online,
      preserving the hierarchy.
    """

    def __init__(
        self,
        graph: GraphClient,
        state: StateDB,
        target_user: str,
        imap_separator: str = "/",
    ) -> None:
        self._graph = graph
        self._state = state
        self._user = target_user
        self._sep = imap_separator

        # Cache: IMAP folder name -> Graph folder ID
        self._cache: dict[str, str] = {}

        # Pre-populate with existing Exchange folders
        self._load_existing_folders()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def resolve(self, imap_folder: str, mailbox: str) -> str:
        """Return the Graph folder ID for the given IMAP folder name.

        Creates the folder (and any parent folders) in Exchange Online if
        it does not exist yet.
        """
        if imap_folder in self._cache:
            return self._cache[imap_folder]

        # Check state DB for a previously-recorded mapping.
        stored = self._state.get_graph_folder_id(mailbox, imap_folder)
        if stored:
            self._cache[imap_folder] = stored
            return stored

        # Try well-known mapping first.
        folder_id = self._try_well_known(imap_folder)
        if folder_id:
            self._cache[imap_folder] = folder_id
            self._state.upsert_folder(mailbox, imap_folder, folder_id)
            log.debug("Mapped '%s' -> well-known folder %s", imap_folder, folder_id)
            return folder_id

        # Custom folder — create it (possibly with hierarchy).
        folder_id = self._create_folder_hierarchy(imap_folder, mailbox)
        return folder_id

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _load_existing_folders(self) -> None:
        """Fetch all existing Exchange Online folders and populate cache."""
        try:
            folders = self._graph.list_mail_folders(self._user)
            for f in folders:
                display_name = f.get("displayName", "")
                folder_id = f.get("id", "")
                if display_name and folder_id:
                    # Store by display name for matching against IMAP names.
                    self._cache[display_name] = folder_id
            log.debug("Loaded %d existing Exchange folders for %s", len(self._cache), self._user)
        except Exception as exc:
            log.warning("Could not pre-load Exchange folders: %s", exc)

    def _try_well_known(self, imap_folder: str) -> Optional[str]:
        """Try to match the IMAP folder to a well-known Exchange folder.

        Checks the full name first, then strips common hierarchy prefixes
        (e.g. ``INBOX.Sent`` → ``Sent``, ``INBOX/Drafts`` → ``Drafts``)
        so that nested IMAP folders map to the correct Exchange well-known
        folders.
        """
        key = imap_folder.lower().strip()
        well_known_name = _WELL_KNOWN.get(key)

        # If exact match failed, try the leaf name after stripping hierarchy
        if not well_known_name:
            # Handle common separators: ".", "/", and the configured separator
            for sep in (".", "/", self._sep):
                if sep in imap_folder:
                    leaf = imap_folder.rsplit(sep, 1)[-1].lower().strip()
                    well_known_name = _WELL_KNOWN.get(leaf)
                    if well_known_name:
                        break

        if not well_known_name:
            return None

        # Use the well-known name directly — Graph API resolves these.
        try:
            resp = self._graph.get(
                f"/users/{self._user}/mailFolders/{well_known_name}"
                "?$select=id,displayName"
            )
            return resp.json()["id"]
        except Exception:
            log.debug("Well-known folder '%s' not found via API", well_known_name)
            return None

    def _create_folder_hierarchy(self, imap_folder: str, mailbox: str) -> str:
        """Create a (possibly nested) folder hierarchy and return the leaf ID."""
        parts = imap_folder.replace(".", self._sep).split(self._sep)
        parent_id: Optional[str] = None
        current_path = ""

        for part in parts:
            current_path = f"{current_path}{self._sep}{part}" if current_path else part

            if current_path in self._cache:
                parent_id = self._cache[current_path]
                continue

            # Check state DB
            stored = self._state.get_graph_folder_id(mailbox, current_path)
            if stored:
                self._cache[current_path] = stored
                parent_id = stored
                continue

            # Check if it already exists in Exchange by display name under the parent
            existing_id = self._find_existing_child(parent_id, part)
            if existing_id:
                self._cache[current_path] = existing_id
                self._state.upsert_folder(mailbox, current_path, existing_id)
                parent_id = existing_id
                continue

            # Create the folder
            folder = self._graph.create_mail_folder(self._user, part, parent_id)
            folder_id = folder["id"]
            self._cache[current_path] = folder_id
            self._state.upsert_folder(mailbox, current_path, folder_id)
            parent_id = folder_id
            log.info("Created folder '%s' in Exchange for %s", current_path, self._user)

        assert parent_id is not None
        # Also cache under the original IMAP folder name
        self._cache[imap_folder] = parent_id
        self._state.upsert_folder(mailbox, imap_folder, parent_id)
        return parent_id

    def _find_existing_child(self, parent_id: Optional[str], display_name: str) -> Optional[str]:
        """Search for a child folder with the given display name."""
        try:
            if parent_id:
                url = (
                    f"/users/{self._user}/mailFolders/{parent_id}/childFolders"
                    f"?$filter=displayName eq '{_escape_odata(display_name)}'"
                    f"&$select=id,displayName"
                )
            else:
                url = (
                    f"/users/{self._user}/mailFolders"
                    f"?$filter=displayName eq '{_escape_odata(display_name)}'"
                    f"&$select=id,displayName"
                )
            resp = self._graph.get(url)
            items = resp.json().get("value", [])
            if items:
                return items[0]["id"]
        except Exception:
            pass
        return None


def _escape_odata(value: str) -> str:
    """Escape a string for use in an OData $filter expression."""
    return value.replace("'", "''")
