"""Microsoft Graph API client with MSAL authentication and retry logic."""

from __future__ import annotations

import base64
import email
import email.policy
import email.utils
import time
from typing import Any, Optional

import msal
import requests

from .config import TargetConfig
from .logger import get_logger

log = get_logger("graph")

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPES = ["https://graph.microsoft.com/.default"]

# Messages larger than this threshold must use an upload session.
LARGE_MESSAGE_THRESHOLD = 3 * 1024 * 1024  # 3 MB (Graph limit is 4 MB; keep margin)


class GraphAPIError(Exception):
    """Raised when a Graph API call fails after retries."""

    def __init__(self, message: str, status_code: int = 0, response_body: str = ""):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class GraphClient:
    """Authenticated HTTP client for Microsoft Graph API."""

    def __init__(self, config: TargetConfig, max_retries: int = 3, request_delay: float = 0.25) -> None:
        self._cfg = config
        self._max_retries = max_retries
        self._request_delay = request_delay
        self._app: Optional[msal.ConfidentialClientApplication] = None
        self._token: Optional[str] = None
        self._token_expires: float = 0.0

    # ------------------------------------------------------------------
    # Authentication (MSAL client-credentials flow)
    # ------------------------------------------------------------------

    def _get_msal_app(self) -> msal.ConfidentialClientApplication:
        if self._app is None:
            authority = f"https://login.microsoftonline.com/{self._cfg.tenant_id}"
            self._app = msal.ConfidentialClientApplication(
                client_id=self._cfg.client_id,
                client_credential=self._cfg.client_secret,
                authority=authority,
            )
            log.debug("MSAL app initialised (tenant=%s)", self._cfg.tenant_id)
        return self._app

    def _ensure_token(self) -> str:
        """Return a valid access token, refreshing if needed."""
        now = time.time()
        if self._token and now < self._token_expires - 60:
            return self._token

        app = self._get_msal_app()
        result = app.acquire_token_for_client(scopes=GRAPH_SCOPES)

        if "access_token" not in result:
            err = result.get("error_description", result.get("error", "unknown"))
            raise GraphAPIError(f"Failed to acquire token: {err}")

        self._token = result["access_token"]
        # Token lifetime is typically 3600s; use the reported value.
        self._token_expires = now + result.get("expires_in", 3600)
        log.debug("Access token acquired (expires in %ds)", result.get("expires_in", 0))
        return self._token

    # ------------------------------------------------------------------
    # Low-level HTTP with retry / back-off
    # ------------------------------------------------------------------

    def _headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        token = self._ensure_token()
        h: dict[str, str] = {"Authorization": f"Bearer {token}"}
        if extra:
            h.update(extra)
        return h

    def _request(
        self,
        method: str,
        url: str,
        *,
        json: Any = None,
        data: Any = None,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
    ) -> requests.Response:
        """Execute an HTTP request with retry on 401 (token refresh), 429, and 5xx."""
        full_url = url if url.startswith("http") else f"{GRAPH_BASE}{url}"
        # Upload-session URLs (outlook.office365.com) are pre-authenticated.
        # Adding our Bearer token to them causes 401, so skip it.
        is_graph_url = full_url.startswith(GRAPH_BASE)
        # Longer timeout for upload-session PUTs (large attachments)
        is_upload_put = (method == "PUT" and not is_graph_url)
        timeout = 300 if is_upload_put else 120
        last_exc: Optional[Exception] = None

        for attempt in range(1, self._max_retries + 1):
            # Build headers fresh each attempt so a refreshed token is used.
            if is_graph_url:
                all_headers = self._headers(headers)
            else:
                # External pre-authenticated URL — no Bearer token.
                all_headers = dict(headers) if headers else {}

            try:
                resp = requests.request(
                    method,
                    full_url,
                    headers=all_headers,
                    json=json,
                    data=data,
                    params=params,
                    timeout=timeout,
                )

                # 401 on a Graph API URL — token likely expired; refresh and retry.
                if resp.status_code == 401 and is_graph_url:
                    log.debug(
                        "HTTP 401 on %s — refreshing token, retry %d/%d",
                        method, attempt, self._max_retries,
                    )
                    self._token = None  # force re-acquisition
                    self._token_expires = 0.0
                    time.sleep(1)
                    continue

                if resp.status_code == 429 or resp.status_code >= 500:
                    retry_after = int(resp.headers.get("Retry-After", 2 ** attempt))
                    log.debug(
                        "HTTP %d on %s — retry %d/%d in %ds",
                        resp.status_code, method,
                        attempt, self._max_retries, retry_after,
                    )
                    time.sleep(retry_after)
                    continue

                if resp.status_code >= 400:
                    raise GraphAPIError(
                        f"HTTP {resp.status_code}: {resp.text[:500]}",
                        status_code=resp.status_code,
                        response_body=resp.text,
                    )

                # Throttle: small delay between successful calls to stay
                # under the Graph API rate limit and avoid 429 storms.
                if self._request_delay > 0:
                    time.sleep(self._request_delay)

                return resp

            except requests.RequestException as exc:
                last_exc = exc
                wait = min(2 ** attempt, 30)
                log.debug(
                    "Request error on %s: %s — retry %d/%d in %ds",
                    method, exc, attempt, self._max_retries, wait,
                )
                time.sleep(wait)

        raise GraphAPIError(
            f"All {self._max_retries} retries exhausted for {method} {full_url}"
        ) from last_exc

    # convenience wrappers
    def get(self, url: str, **kw: Any) -> requests.Response:
        return self._request("GET", url, **kw)

    def post(self, url: str, **kw: Any) -> requests.Response:
        return self._request("POST", url, **kw)

    def patch(self, url: str, **kw: Any) -> requests.Response:
        return self._request("PATCH", url, **kw)

    def put(self, url: str, **kw: Any) -> requests.Response:
        return self._request("PUT", url, **kw)

    # ------------------------------------------------------------------
    # Mail folder operations
    # ------------------------------------------------------------------

    def list_mail_folders(self, user_id: str) -> list[dict]:
        """Return all mail folders (including children) for a user."""
        folders: list[dict] = []
        url = f"/users/{user_id}/mailFolders?$top=100&$select=id,displayName,parentFolderId"
        while url:
            resp = self.get(url)
            body = resp.json()
            folders.extend(body.get("value", []))
            url = body.get("@odata.nextLink")
            # Also fetch child folders
        # For each folder, also fetch child folders recursively
        all_folders = list(folders)
        for f in folders:
            all_folders.extend(self._list_child_folders(user_id, f["id"]))
        return all_folders

    def _list_child_folders(self, user_id: str, parent_id: str) -> list[dict]:
        children: list[dict] = []
        url = f"/users/{user_id}/mailFolders/{parent_id}/childFolders?$top=100&$select=id,displayName,parentFolderId"
        while url:
            resp = self.get(url)
            body = resp.json()
            page = body.get("value", [])
            children.extend(page)
            url = body.get("@odata.nextLink")
            for child in page:
                children.extend(self._list_child_folders(user_id, child["id"]))
        return children

    def create_mail_folder(self, user_id: str, display_name: str, parent_folder_id: str | None = None) -> dict:
        """Create a mail folder and return its JSON representation."""
        payload: dict[str, str] = {"displayName": display_name, "isHidden": False}
        if parent_folder_id:
            url = f"/users/{user_id}/mailFolders/{parent_folder_id}/childFolders"
        else:
            url = f"/users/{user_id}/mailFolders"
        resp = self.post(url, json=payload)
        folder = resp.json()
        log.info("Created folder '%s' (id=%s) for %s", display_name, folder.get("id"), user_id)
        return folder

    def fetch_all_message_ids(self, user_id: str) -> dict[str, str]:
        """Return a mapping of internetMessageId → Graph id for ALL messages in a mailbox.

        This is used by the purge command to match IMAP Message-IDs against
        M365 messages for precise deletion.
        """
        id_map: dict[str, str] = {}
        url = f"/users/{user_id}/messages?$select=id,internetMessageId&$top=1000"
        page = 0
        while url:
            resp = self.get(url)
            body = resp.json()
            for msg in body.get("value", []):
                mid = msg.get("internetMessageId", "")
                if mid:
                    id_map[mid] = msg["id"]
            url = body.get("@odata.nextLink")
            page += 1
            if page % 10 == 0:
                log.debug("  Fetched %d message IDs from %s so far...", len(id_map), user_id)
        log.info("Fetched %d message IDs from %s", len(id_map), user_id)
        return id_map

    def fetch_draft_messages(self, user_id: str) -> list[dict]:
        """Return all messages marked as draft in a mailbox.

        Each item includes the fields needed to re-create the message
        as a non-draft.
        """
        drafts: list[dict] = []
        url = (
            f"/users/{user_id}/messages"
            f"?$filter=isDraft eq true"
            f"&$select=id,isRead,hasAttachments,receivedDateTime,"
            f"sentDateTime,parentFolderId"
            f"&$top=1000"
        )
        page = 0
        while url:
            resp = self.get(url)
            body = resp.json()
            drafts.extend(body.get("value", []))
            url = body.get("@odata.nextLink")
            page += 1
            if page % 5 == 0:
                log.debug("  Fetched %d draft messages from %s so far...",
                          len(drafts), user_id)
        log.info("Found %d draft messages in %s", len(drafts), user_id)
        return drafts

    def recreate_as_non_draft(
        self, user_id: str, msg: dict,
    ) -> str:
        """Delete a draft and re-create it as a non-draft message.

        PATCHing ``PR_MESSAGE_FLAGS`` does **not** clear ``isDraft`` on
        existing messages in Exchange Online.  The only Graph-API approach
        that works is to create a *new* message via JSON ``POST`` with
        ``singleValueExtendedProperties`` included in the creation payload.

        Steps:
        1. GET full message JSON (subject, body, headers, etc.)
        2. GET attachments (if any)
        3. POST new message with ``Integer 0x0E07`` = "0"/"1"
        4. POST attachments to the new message
        5. MOVE to the original folder
        6. DELETE the original draft

        Returns the new message ID.
        """
        msg_id = msg["id"]
        parent_folder_id = msg.get("parentFolderId", "")

        # ── 1. Get the full message content ──────────────────────────────
        select = (
            "subject,body,from,sender,toRecipients,ccRecipients,"
            "bccRecipients,replyTo,importance,categories,isRead,"
            "hasAttachments,receivedDateTime,sentDateTime,"
            "parentFolderId,internetMessageHeaders"
        )
        resp = self.get(f"/users/{user_id}/messages/{msg_id}?$select={select}")
        old = resp.json()

        # ── 2. Get attachments ───────────────────────────────────────────
        attachments: list[dict] = []
        if old.get("hasAttachments"):
            att_url = f"/users/{user_id}/messages/{msg_id}/attachments?$top=100"
            while att_url:
                att_resp = self.get(att_url)
                att_body = att_resp.json()
                attachments.extend(att_body.get("value", []))
                att_url = att_body.get("@odata.nextLink")

        # ── 3. Build the new message payload ─────────────────────────────
        is_read = old.get("isRead", False)
        mapi_flags = "1" if is_read else "0"
        ts_deliver = old.get("receivedDateTime") or "2025-01-01T00:00:00Z"
        ts_submit = old.get("sentDateTime") or ts_deliver

        new_msg: dict = {
            "subject": old.get("subject", ""),
            "body": old.get("body", {"contentType": "text", "content": ""}),
            "toRecipients": old.get("toRecipients", []),
            "ccRecipients": old.get("ccRecipients", []),
            "bccRecipients": old.get("bccRecipients", []),
            "replyTo": old.get("replyTo", []),
            "importance": old.get("importance", "normal"),
            "categories": old.get("categories", []),
            "isRead": is_read,
            "singleValueExtendedProperties": [
                {"id": "Integer 0x0E07", "value": mapi_flags},
                {"id": "SystemTime 0x0E06", "value": ts_deliver},
                {"id": "SystemTime 0x0039", "value": ts_submit},
            ],
        }

        # Preserve original sender (from / sender)
        if old.get("from"):
            new_msg["from"] = old["from"]
        if old.get("sender"):
            new_msg["sender"] = old["sender"]

        # Graph API only allows custom headers (x-*) on POST, max 5.
        # Standard headers (Message-ID, Date, etc.) are handled by
        # Graph's own fields.
        if old.get("internetMessageHeaders"):
            x_headers = [
                h for h in old["internetMessageHeaders"]
                if h.get("name", "").lower().startswith("x-")
            ]
            if x_headers:
                new_msg["internetMessageHeaders"] = x_headers[:5]

        # Include small file attachments directly in the creation payload
        # (max ~3 MB each).  Larger ones are uploaded separately below.
        small_atts: list[dict] = []
        large_atts: list[dict] = []
        for att in attachments:
            if att.get("@odata.type") == "#microsoft.graph.fileAttachment":
                content = att.get("contentBytes", "")
                if content and len(content) < 3_500_000:  # ~2.6 MB decoded
                    small_atts.append({
                        "@odata.type": "#microsoft.graph.fileAttachment",
                        "name": att.get("name", "attachment"),
                        "contentBytes": content,
                        "contentType": att.get("contentType", "application/octet-stream"),
                        "isInline": att.get("isInline", False),
                        **({"contentId": att["contentId"]} if att.get("contentId") else {}),
                    })
                else:
                    large_atts.append(att)
            elif att.get("@odata.type") == "#microsoft.graph.itemAttachment":
                # Item attachments (embedded messages) — skip contentBytes
                # and just carry over the basic metadata.
                large_atts.append(att)

        if small_atts:
            new_msg["attachments"] = small_atts

        # ── 4. Create the new message ────────────────────────────────────
        create_resp = self.post(f"/users/{user_id}/messages", json=new_msg)
        new_id = create_resp.json()["id"]

        # ── 5. Upload large attachments separately ───────────────────────
        for att in large_atts:
            try:
                if att.get("@odata.type") == "#microsoft.graph.fileAttachment":
                    att_payload = {
                        "@odata.type": "#microsoft.graph.fileAttachment",
                        "name": att.get("name", "attachment"),
                        "contentBytes": att.get("contentBytes", ""),
                        "contentType": att.get("contentType", "application/octet-stream"),
                        "isInline": att.get("isInline", False),
                    }
                    if att.get("contentId"):
                        att_payload["contentId"] = att["contentId"]
                    self.post(
                        f"/users/{user_id}/messages/{new_id}/attachments",
                        json=att_payload,
                    )
            except Exception as exc:
                log.warning("Could not copy attachment '%s': %s",
                            att.get("name", "?"), exc)

        # ── 6. Move to the original folder ───────────────────────────────
        folder_id = parent_folder_id or old.get("parentFolderId")
        if folder_id:
            try:
                move_resp = self.post(
                    f"/users/{user_id}/messages/{new_id}/move",
                    json={"destinationId": folder_id},
                )
                new_id = move_resp.json().get("id", new_id)
            except Exception as exc:
                log.warning("Move failed for new message: %s", exc)

        # ── 7. Delete the original draft ─────────────────────────────────
        self._request("DELETE", f"/users/{user_id}/messages/{msg_id}")

        return new_id

    def verify_non_draft(self, user_id: str, msg_id: str) -> tuple[bool, dict]:
        """Check isDraft and from on a re-created message.

        Returns ``(is_fixed, details)`` where ``details`` has
        ``isDraft``, ``from``, ``subject``.
        """
        try:
            resp = self.get(
                f"/users/{user_id}/messages/{msg_id}"
                f"?$select=isDraft,from,subject,receivedDateTime"
            )
            data = resp.json()
            is_fixed = data.get("isDraft") is False
            return is_fixed, data
        except Exception as exc:
            return False, {"error": str(exc)}

    # ------------------------------------------------------------------
    # Message upload
    # ------------------------------------------------------------------

    def upload_mime_message(
        self,
        user_id: str,
        folder_id: str,
        mime_content: bytes,
        *,
        is_read: bool = False,
        is_flagged: bool = False,
        imap_date_iso: str | None = None,
    ) -> dict:
        """Upload a MIME message as a **non-draft** and move it to *folder_id*.

        All messages are parsed from MIME into a Graph JSON payload that
        includes ``singleValueExtendedProperties`` with ``Integer 0x0E07``
        set at creation time — the only approach that prevents Exchange
        Online from marking the message as a draft.

        Steps:
        1. Parse MIME with ``email.message_from_bytes()``.
        2. Build JSON via ``_mime_to_graph_json()`` (includes MAPI flags).
        3. POST JSON to ``/users/{id}/messages``.
        4. Upload attachments (small ones inline, large via upload sessions).
        5. Move to the target *folder_id*.
        """
        msg = email.message_from_bytes(mime_content, policy=email.policy.default)

        # Build JSON payload with extended properties for non-draft
        payload = _mime_to_graph_json(
            msg,
            is_read=is_read,
            is_flagged=is_flagged,
            imap_date_iso=imap_date_iso,
        )

        # Create the message (without attachments initially)
        url = f"/users/{user_id}/messages"
        resp = self.post(url, json=payload)
        created = resp.json()
        message_id = created["id"]

        # Upload attachments — delete orphan on failure
        try:
            self._upload_attachments(user_id, message_id, msg)
        except Exception:
            self._try_delete(user_id, message_id)
            raise

        # Move into the target folder — delete orphan on failure
        try:
            moved = self._move_message(user_id, message_id, folder_id)
        except Exception:
            self._try_delete(user_id, message_id)
            raise

        return moved

    def _try_delete(self, user_id: str, message_id: str) -> None:
        """Best-effort delete of an orphaned message to avoid duplicates."""
        try:
            self._request("DELETE", f"/users/{user_id}/messages/{message_id}")
            log.debug("Cleaned up orphaned message %s", message_id[:30])
        except Exception:
            pass

    def _upload_attachments(
        self,
        user_id: str,
        message_id: str,
        msg: email.message.Message,
    ) -> None:
        """Extract and upload attachments from a parsed MIME message."""
        for part in msg.walk():
            content_disposition = str(part.get("Content-Disposition", ""))
            if part.get_content_maintype() == "multipart":
                continue
            if "attachment" not in content_disposition and "inline" not in content_disposition:
                continue

            filename = part.get_filename() or "attachment"
            content_bytes = part.get_payload(decode=True)
            if content_bytes is None:
                continue
            content_type = part.get_content_type()
            is_inline = "inline" in content_disposition
            content_id = (part.get("Content-ID") or "").strip("<>")

            if len(content_bytes) <= LARGE_MESSAGE_THRESHOLD:
                # Small attachment — upload inline via JSON
                attach_payload: dict[str, Any] = {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": filename,
                    "contentType": content_type,
                    "contentBytes": base64.b64encode(content_bytes).decode("ascii"),
                    "isInline": is_inline,
                }
                if content_id:
                    attach_payload["contentId"] = content_id
                self.post(
                    f"/users/{user_id}/messages/{message_id}/attachments",
                    json=attach_payload,
                )
            else:
                # Large attachment — upload session
                self._upload_large_attachment(
                    user_id, message_id, filename, content_type, content_bytes
                )
            log.debug("  Attached '%s' (%d bytes)", filename, len(content_bytes))

    def _upload_large_attachment(
        self,
        user_id: str,
        message_id: str,
        filename: str,
        content_type: str,
        content_bytes: bytes,
    ) -> None:
        """Upload a single large attachment via an upload session.

        Uses 3 MB chunks with per-chunk retry.  If the session itself
        fails, it is re-created up to 2 times.
        """
        total = len(content_bytes)
        chunk_size = 3 * 1024 * 1024  # 3 MB chunks

        for session_attempt in range(3):
            session_url = (
                f"/users/{user_id}/messages/{message_id}"
                "/attachments/createUploadSession"
            )
            session_payload = {
                "AttachmentItem": {
                    "attachmentType": "file",
                    "name": filename,
                    "size": total,
                    "contentType": content_type,
                }
            }
            session_resp = self.post(session_url, json=session_payload)
            upload_url = session_resp.json()["uploadUrl"]

            try:
                offset = 0
                while offset < total:
                    end = min(offset + chunk_size, total)
                    chunk = content_bytes[offset:end]
                    content_range = f"bytes {offset}-{end - 1}/{total}"
                    self.put(
                        upload_url,
                        data=chunk,
                        headers={
                            "Content-Type": "application/octet-stream",
                            "Content-Range": content_range,
                            "Content-Length": str(len(chunk)),
                        },
                    )
                    offset = end
                return  # success
            except Exception:
                if session_attempt < 2:
                    log.debug(
                        "Upload session failed for '%s' (%d bytes), "
                        "retrying with new session (%d/3)",
                        filename, total, session_attempt + 2,
                    )
                    time.sleep(2 ** (session_attempt + 1))
                    continue
                raise

    # --- move message to target folder -----------------------------------

    def _move_message(self, user_id: str, message_id: str, destination_folder_id: str) -> dict:
        """Move a message to a different mail folder."""
        url = f"/users/{user_id}/messages/{message_id}/move"
        resp = self.post(url, json={"destinationId": destination_folder_id})
        return resp.json()

    # ------------------------------------------------------------------
    # Message metadata updates
    # ------------------------------------------------------------------

    def update_message(self, user_id: str, message_id: str, properties: dict) -> dict:
        """PATCH a message to update properties like isRead."""
        url = f"/users/{user_id}/messages/{message_id}"
        resp = self.patch(url, json=properties)
        return resp.json()

    def set_message_read_state(self, user_id: str, message_id: str, is_read: bool) -> None:
        """Mark a message as read or unread."""
        self.update_message(user_id, message_id, {"isRead": is_read})

    # ------------------------------------------------------------------
    # User validation
    # ------------------------------------------------------------------

    def validate_user(self, user_id: str) -> bool:
        """Check that a user exists and the app has access."""
        try:
            self.get(f"/users/{user_id}?$select=id,displayName,mail")
            return True
        except GraphAPIError as exc:
            if exc.status_code == 404:
                return False
            raise


# ---------------------------------------------------------------------------
# MIME → Graph JSON conversion (used for large messages)
# ---------------------------------------------------------------------------

def _parse_address_list(header_value: str | None) -> list[dict]:
    """Parse an RFC 2822 address header into Graph API recipient format."""
    if not header_value:
        return []
    recipients = []
    for display_name, addr in email.utils.getaddresses([header_value]):
        if addr:
            entry: dict[str, Any] = {"emailAddress": {"address": addr}}
            if display_name:
                entry["emailAddress"]["name"] = display_name
            recipients.append(entry)
    return recipients


def _extract_body(msg: email.message.Message) -> dict:
    """Extract the best body (HTML preferred, fallback to plain text)."""
    html_body: str | None = None
    text_body: str | None = None

    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            disp = str(part.get("Content-Disposition", ""))
            if "attachment" in disp:
                continue
            payload = part.get_payload(decode=True)
            if payload is None:
                continue
            charset = part.get_content_charset() or "utf-8"
            try:
                decoded = payload.decode(charset, errors="replace")
            except (LookupError, UnicodeDecodeError):
                decoded = payload.decode("utf-8", errors="replace")
            if ct == "text/html" and not html_body:
                html_body = decoded
            elif ct == "text/plain" and not text_body:
                text_body = decoded
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            try:
                decoded = payload.decode(charset, errors="replace")
            except (LookupError, UnicodeDecodeError):
                decoded = payload.decode("utf-8", errors="replace")
            if msg.get_content_type() == "text/html":
                html_body = decoded
            else:
                text_body = decoded

    if html_body:
        return {"contentType": "html", "content": html_body}
    return {"contentType": "text", "content": text_body or ""}


def _mime_to_graph_json(
    msg: email.message.Message,
    *,
    is_read: bool = False,
    is_flagged: bool = False,
    imap_date_iso: str | None = None,
) -> dict[str, Any]:
    """Convert a parsed MIME message into a Graph API message JSON payload.

    When ``imap_date_iso`` and ``is_read`` are provided, the payload
    includes ``singleValueExtendedProperties`` with:

    - ``Integer 0x0E07`` (PR_MESSAGE_FLAGS) — clears MSGFLAG_UNSENT so
      the message is **not** created as a draft.
    - ``SystemTime 0x0E06`` (PR_MESSAGE_DELIVERY_TIME) — original date.
    - ``SystemTime 0x0039`` (PR_CLIENT_SUBMIT_TIME) — original date.
    """
    payload: dict[str, Any] = {
        "subject": msg.get("Subject", "(no subject)"),
        "body": _extract_body(msg),
        "toRecipients": _parse_address_list(msg.get("To")),
        "ccRecipients": _parse_address_list(msg.get("Cc")),
        "bccRecipients": _parse_address_list(msg.get("Bcc")),
        "isRead": is_read,
    }

    # From
    from_list = _parse_address_list(msg.get("From"))
    if from_list:
        payload["from"] = from_list[0]
        payload["sender"] = from_list[0]

    # Date — prefer the IMAP INTERNALDATE (already UTC ISO), fall back
    # to the MIME Date header.
    iso_date = imap_date_iso
    if not iso_date:
        date_str = msg.get("Date")
        if date_str:
            try:
                parsed_dt = email.utils.parsedate_to_datetime(date_str)
                iso_date = parsed_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                pass

    # Reply-To
    reply_to = _parse_address_list(msg.get("Reply-To"))
    if reply_to:
        payload["replyTo"] = reply_to

    # Importance
    importance = (msg.get("Importance") or msg.get("X-Priority") or "").lower()
    if "high" in importance or importance in ("1", "2"):
        payload["importance"] = "high"
    elif "low" in importance or importance in ("4", "5"):
        payload["importance"] = "low"
    if is_flagged:
        payload["importance"] = "high"

    # --- Extended MAPI properties (the key to creating non-drafts) ---
    mapi_flags = "1" if is_read else "0"
    extended_props: list[dict[str, str]] = [
        {"id": "Integer 0x0E07", "value": mapi_flags},
    ]
    if iso_date:
        extended_props.append({"id": "SystemTime 0x0E06", "value": iso_date})
        extended_props.append({"id": "SystemTime 0x0039", "value": iso_date})
    payload["singleValueExtendedProperties"] = extended_props

    return payload
