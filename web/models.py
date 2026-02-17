"""Pydantic models for the web API."""

from __future__ import annotations

from pydantic import BaseModel, Field
from typing import Optional


# ---------------------------------------------------------------------------
# Configuration models
# ---------------------------------------------------------------------------

class SourceConfigModel(BaseModel):
    host: str = ""
    port: int = 993
    ssl: bool = True
    password: str = ""  # optional fallback password


class TargetConfigModel(BaseModel):
    tenant_id: str = ""
    client_id: str = ""
    client_secret: str = ""


class MailboxModel(BaseModel):
    id: Optional[int] = None
    source_user: str
    target_user: str
    source_password: str = ""
    include_folders: Optional[list[str]] = None
    exclude_folders: Optional[list[str]] = None


class OptionsModel(BaseModel):
    batch_size: int = 50
    max_retries: int = 3
    max_workers: int = 4
    request_delay: float = 0.25
    log_level: str = "INFO"
    exclude_folders: list[str] = Field(default_factory=list)


class FullConfigModel(BaseModel):
    source: SourceConfigModel = Field(default_factory=SourceConfigModel)
    target: TargetConfigModel = Field(default_factory=TargetConfigModel)
    mailboxes: list[MailboxModel] = Field(default_factory=list)
    options: OptionsModel = Field(default_factory=OptionsModel)


# ---------------------------------------------------------------------------
# Migration status models
# ---------------------------------------------------------------------------

class MigrationProgress(BaseModel):
    """Real-time progress update sent via WebSocket."""
    event: str  # "progress", "folder_start", "folder_done", "mailbox_start", "mailbox_done", "complete", "error", "log"
    mailbox: str = ""
    folder: str = ""
    current: int = 0
    total: int = 0
    migrated: int = 0
    skipped: int = 0
    failed: int = 0
    message: str = ""


class RunSummary(BaseModel):
    """Summary of a completed migration run."""
    run_id: int
    started_at: str
    ended_at: Optional[str] = None
    status: str
    mailboxes: list[MailboxRunStats] = Field(default_factory=list)


class MailboxRunStats(BaseModel):
    mailbox: str
    folders_processed: int = 0
    messages_total: int = 0
    messages_migrated: int = 0
    messages_skipped: int = 0
    messages_failed: int = 0
    success_rate: float = 0.0


# Need to rebuild RunSummary after MailboxRunStats is defined
RunSummary.model_rebuild()


class JobStatus(BaseModel):
    """Current state of a background job."""
    job_id: str
    type: str  # "migrate", "repair", "merge"
    running: bool
    mailbox: str = ""
    progress_pct: float = 0.0
    message: str = ""


class StatsOverview(BaseModel):
    """Dashboard overview statistics."""
    total_mailboxes: int = 0
    total_messages: int = 0
    migrated: int = 0
    failed: int = 0
    skipped: int = 0
    total_runs: int = 0
    last_run_status: str = ""
    last_run_date: str = ""
