"""Configuration loader and validator for the migration tool."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class SourceConfig:
    """IMAP source server settings."""
    host: str
    port: int = 993
    ssl: bool = True
    username: str = ""
    password: str = ""


@dataclass
class TargetConfig:
    """Microsoft 365 / Graph API target settings."""
    tenant_id: str = ""
    client_id: str = ""
    client_secret: str = ""


@dataclass
class MailboxMapping:
    """Maps one source IMAP user to a target M365 user."""
    source_user: str
    target_user: str
    source_password: str = ""
    old_target_user: str = ""  # used by --relocate to delete from the wrong mailbox
    include_folders: Optional[list[str]] = None
    exclude_folders: Optional[list[str]] = None


@dataclass
class Options:
    """Runtime options."""
    batch_size: int = 50
    max_retries: int = 3
    max_workers: int = 4         # concurrent Graph API upload threads
    request_delay: float = 0.25  # seconds between Graph API calls
    log_level: str = "INFO"
    state_db: str = "migration_state.db"
    log_file: str = "migration.log"
    exclude_folders: Optional[list[str]] = None  # global folder exclusions


@dataclass
class AppConfig:
    """Top-level application configuration."""
    source: SourceConfig = field(default_factory=SourceConfig)
    target: TargetConfig = field(default_factory=TargetConfig)
    mailboxes: list[MailboxMapping] = field(default_factory=list)
    options: Options = field(default_factory=Options)


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def _apply_env_overrides(cfg: AppConfig) -> None:
    """Override secrets with environment variables when set."""
    imap_pw = os.environ.get("IMAP_PASSWORD")
    if imap_pw:
        cfg.source.password = imap_pw

    graph_secret = os.environ.get("GRAPH_CLIENT_SECRET")
    if graph_secret:
        cfg.target.client_secret = graph_secret

    graph_tenant = os.environ.get("GRAPH_TENANT_ID")
    if graph_tenant:
        cfg.target.tenant_id = graph_tenant

    graph_client = os.environ.get("GRAPH_CLIENT_ID")
    if graph_client:
        cfg.target.client_id = graph_client


def _parse_mailbox(raw: dict) -> MailboxMapping:
    return MailboxMapping(
        source_user=raw["source_user"],
        target_user=raw["target_user"],
        source_password=raw.get("source_password", ""),
        old_target_user=raw.get("old_target_user", ""),
        include_folders=raw.get("include_folders"),
        exclude_folders=raw.get("exclude_folders"),
    )


def load_config(path: str | Path = "config.yaml") -> AppConfig:
    """Load configuration from a YAML file, apply env-var overrides, and validate."""
    path = Path(path)
    if not path.exists():
        print(f"Error: configuration file not found: {path}", file=sys.stderr)
        sys.exit(1)

    with open(path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    # --- source ---
    src_raw = raw.get("source", {})
    source = SourceConfig(
        host=src_raw.get("host", ""),
        port=int(src_raw.get("port", 993)),
        ssl=bool(src_raw.get("ssl", True)),
        username=src_raw.get("username", ""),
        password=src_raw.get("password", ""),
    )

    # --- target ---
    tgt_raw = raw.get("target", {})
    target = TargetConfig(
        tenant_id=tgt_raw.get("tenant_id", ""),
        client_id=tgt_raw.get("client_id", ""),
        client_secret=tgt_raw.get("client_secret", ""),
    )

    # --- mailboxes ---
    mailboxes = [_parse_mailbox(m) for m in raw.get("mailboxes", [])]

    # --- options ---
    opt_raw = raw.get("options", {})
    options = Options(
        batch_size=int(opt_raw.get("batch_size", 50)),
        max_retries=int(opt_raw.get("max_retries", 3)),
        max_workers=int(opt_raw.get("max_workers", 4)),
        request_delay=float(opt_raw.get("request_delay", 0.25)),
        log_level=str(opt_raw.get("log_level", "INFO")).upper(),
        state_db=str(opt_raw.get("state_db", "migration_state.db")),
        log_file=str(opt_raw.get("log_file", "migration.log")),
        exclude_folders=opt_raw.get("exclude_folders"),
    )

    cfg = AppConfig(source=source, target=target, mailboxes=mailboxes, options=options)

    # Environment variables take precedence over file values.
    _apply_env_overrides(cfg)

    # Fill in per-mailbox passwords from the global fallback.
    _resolve_mailbox_passwords(cfg)

    # Validate
    _validate(cfg)

    return cfg


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

class ConfigError(Exception):
    """Raised when the configuration is invalid."""


def _resolve_mailbox_passwords(cfg: AppConfig) -> None:
    """Fill in per-mailbox passwords from the global fallback if not set."""
    for mb in cfg.mailboxes:
        if not mb.source_password and cfg.source.password:
            mb.source_password = cfg.source.password


def _validate(cfg: AppConfig) -> None:
    errors: list[str] = []

    if not cfg.source.host:
        errors.append("source.host is required")

    if not cfg.target.tenant_id:
        errors.append("target.tenant_id is required (set in config or GRAPH_TENANT_ID env var)")
    if not cfg.target.client_id:
        errors.append("target.client_id is required (set in config or GRAPH_CLIENT_ID env var)")
    if not cfg.target.client_secret:
        errors.append("target.client_secret is required (set in config or GRAPH_CLIENT_SECRET env var)")

    if not cfg.mailboxes:
        errors.append("At least one mailbox mapping is required under 'mailboxes'")

    for i, mb in enumerate(cfg.mailboxes):
        if not mb.source_user:
            errors.append(f"mailboxes[{i}].source_user is required")
        if not mb.target_user:
            errors.append(f"mailboxes[{i}].target_user is required")
        if not mb.source_password:
            errors.append(
                f"mailboxes[{i}].source_password is required "
                "(set per-mailbox or provide a default in source.password / IMAP_PASSWORD env var)"
            )

    if cfg.options.batch_size < 1:
        errors.append("options.batch_size must be >= 1")

    if errors:
        msg = "Configuration errors:\n" + "\n".join(f"  - {e}" for e in errors)
        raise ConfigError(msg)
