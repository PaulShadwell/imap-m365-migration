# IMAP to Microsoft 365 Migration Tool

A Python script that migrates email from an IMAP server to Microsoft 365
Exchange Online via the Microsoft Graph API, preserving folder structure,
message content, read/unread state, and flags.

## Features

- **Full-fidelity MIME import** — raw RFC 822 messages are uploaded to Exchange
  Online, preserving all original headers, formatting, and attachments.
- **Folder mapping** — standard folders (Inbox, Sent, Drafts, Trash, Junk) are
  mapped to their Exchange equivalents; custom folders are recreated
  automatically.
- **Resume support** — a local SQLite database tracks every migrated message.
  If the script is interrupted, re-running it picks up where it left off.
- **Rate-limit handling** — respects Microsoft Graph throttling with exponential
  back-off and `Retry-After` headers.
- **Large message support** — messages over 3 MB use Graph upload sessions.
- **Rich console output** — progress bars, colour-coded logs, and a summary
  table at the end.
- **Dry-run mode** — validate your configuration and test connectivity before
  migrating a single message.

---

## Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.10 or later |
| pip | any recent version |
| An Azure AD (Entra ID) tenant | with admin consent rights |
| Source IMAP server | accessible from the machine running the script |

---

## Quick Start

```bash
# 1. Clone or download this project
cd MailboxMigration

# 2. Create a virtual environment (recommended)
python3 -m venv .venv
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate         # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Copy and edit the config file
cp config.example.yaml config.yaml
# Edit config.yaml with your IMAP and Azure details (see below)

# 5. Validate everything works
python migrate.py --dry-run

# 6. Run the migration
python migrate.py
```

---

## Azure AD (Entra ID) App Registration

The tool authenticates to Microsoft Graph using the **OAuth 2.0 client
credentials** flow. You need to register an application in Azure and grant it
the required permissions.

### Step-by-step

1. **Sign in** to the [Azure Portal](https://portal.azure.com) with a Global
   Administrator or Application Administrator account.

2. Navigate to **Microsoft Entra ID** > **App registrations** > **New
   registration**.

3. Fill in the form:
   - **Name:** `IMAP Migration Tool` (or any name you prefer)
   - **Supported account types:** *Accounts in this organizational directory
     only*
   - **Redirect URI:** leave blank (not needed for client credentials)
   - Click **Register**.

4. On the app's **Overview** page, note:
   - **Application (client) ID** — this is `client_id`
   - **Directory (tenant) ID** — this is `tenant_id`

5. Go to **Certificates & secrets** > **Client secrets** > **New client
   secret**:
   - Add a description and choose an expiry.
   - Copy the **Value** immediately (it won't be shown again). This is
     `client_secret`.

6. Go to **API permissions** > **Add a permission** > **Microsoft Graph** >
   **Application permissions**:
   - Search for and add **`Mail.ReadWrite`**.
   - If you also want to validate users: add **`User.Read.All`**.

7. Click **Grant admin consent for \<your tenant\>** and confirm.

### Summary of required permissions

| Permission | Type | Purpose |
|---|---|---|
| `Mail.ReadWrite` | Application | Create folders and import messages |
| `User.Read.All` | Application | Validate target user exists (optional) |

---

## Configuration

Copy `config.example.yaml` to `config.yaml` and fill in the values:

```yaml
source:
  host: imap.example.com
  port: 993
  ssl: true
  # Optional fallback password (used for any mailbox without its own).
  # password: "shared-password"

target:
  tenant_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
  client_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
  client_secret: "your-client-secret"

mailboxes:
  - source_user: alice@example.com
    target_user: alice@company.onmicrosoft.com
    source_password: "alices-imap-password"
  - source_user: bob@example.com
    target_user: bob@company.onmicrosoft.com
    source_password: "bobs-imap-password"
    exclude_folders:
      - Trash
      - Junk

options:
  batch_size: 50
  max_retries: 3
  log_level: INFO
  state_db: migration_state.db
  log_file: migration.log
```

### Environment variable overrides

Secrets can be provided via environment variables instead of (or in addition to)
the config file. Environment variables take precedence:

| Variable | Overrides |
|---|---|
| `IMAP_PASSWORD` | `source.password` (default fallback for all mailboxes) |
| `GRAPH_TENANT_ID` | `target.tenant_id` |
| `GRAPH_CLIENT_ID` | `target.client_id` |
| `GRAPH_CLIENT_SECRET` | `target.client_secret` |

Example:

```bash
export IMAP_PASSWORD="secret"
export GRAPH_CLIENT_SECRET="azure-secret"
python migrate.py
```

### Mailbox options

Each entry under `mailboxes` supports:

| Field | Required | Description |
|---|---|---|
| `source_user` | Yes | IMAP username / email address |
| `source_password` | Yes* | IMAP password for this mailbox (*falls back to `source.password` if omitted) |
| `target_user` | Yes | Microsoft 365 user principal name (UPN) |
| `include_folders` | No | Only migrate these folders (list) |
| `exclude_folders` | No | Skip these folders (list) |

---

## Usage

### Full migration

```bash
python migrate.py
```

### Dry run (validate config and connectivity)

```bash
python migrate.py --dry-run
```

### Custom config path

```bash
python migrate.py --config /path/to/my-config.yaml
```

### Re-running after interruption

Simply run the same command again. The SQLite state database tracks which
messages were already migrated, so only pending messages will be processed.

---

## How It Works

```
Source IMAP Server
       │
       │  1. Connect & authenticate
       │  2. List folders
       │  3. Fetch message UIDs
       │  4. Fetch raw RFC822 content
       ▼
   Migration Script
       │
       │  5. Map IMAP folders → Exchange folders
       │  6. Create missing folders via Graph API
       │  7. Upload MIME messages via Graph API
       │  8. Sync read/unread & flagged state
       │  9. Track progress in SQLite
       ▼
Microsoft 365 Exchange Online
```

### Folder mapping

Standard IMAP folder names are automatically mapped to their Exchange Online
equivalents:

| IMAP folder | Exchange Online |
|---|---|
| `INBOX` | Inbox |
| `Sent`, `Sent Items`, `Sent Mail`, `[Gmail]/Sent Mail` | Sent Items |
| `Drafts`, `[Gmail]/Drafts` | Drafts |
| `Trash`, `Deleted Items`, `[Gmail]/Trash` | Deleted Items |
| `Junk`, `Spam`, `Junk E-mail`, `[Gmail]/Spam` | Junk Email |
| `Archive`, `[Gmail]/All Mail` | Archive |

All other folders are created as custom mail folders in Exchange Online,
preserving the original hierarchy.

### Message fidelity

Messages are uploaded using the Graph API's MIME import endpoint, which
preserves:

- All original headers (From, To, Date, Message-ID, etc.)
- HTML and plain-text body
- All attachments
- Inline images
- Read/unread state (synced after upload)
- Flagged/important state (synced after upload)

---

## Output Files

| File | Purpose |
|---|---|
| `migration_state.db` | SQLite database tracking migration progress |
| `migration.log` | Detailed log file (DEBUG level) |

---

## Troubleshooting

### "Failed to acquire token"

- Double-check `tenant_id`, `client_id`, and `client_secret`.
- Ensure admin consent has been granted for the required permissions.
- Verify the client secret hasn't expired.

### "Target user not found in Microsoft 365"

- Ensure the `target_user` value matches the user's User Principal Name (UPN)
  in Microsoft 365. This is usually their email address or
  `user@tenant.onmicrosoft.com`.
- Ensure the app has `User.Read.All` permission.

### "HTTP 403: Insufficient privileges"

- The app registration is missing the required `Mail.ReadWrite` permission, or
  admin consent was not granted.

### "IMAP connection failed"

- Verify the IMAP host and port are correct.
- Check that IMAP access is enabled on the source mail server.
- If using a firewall, ensure outbound port 993 (or your configured port) is
  open.

### Partial migration / resuming

The tool is designed to be re-run safely. Each successfully migrated message is
recorded in `migration_state.db`. On subsequent runs, already-migrated messages
are automatically skipped. If some messages failed, check `migration.log` for
details, fix the issue, and run again.

### Rate limiting / throttling

Microsoft Graph API has throttling limits. The tool handles `429 Too Many
Requests` responses automatically with exponential back-off. If you're migrating
large mailboxes, the process may slow down during peak throttling. You can
reduce `batch_size` in the config to be gentler on the API.

---

## Web GUI

The tool also includes a full web-based GUI built with FastAPI, Alpine.js, and
Tailwind CSS. This makes it easier for a team to use without the command line.

### Features

- **Dashboard** — overview of migration stats, active job progress, quick
  actions, and recent run history.
- **Configuration** — edit IMAP source, Microsoft 365 target, and options
  directly from the browser.  Passwords are masked in the UI.
- **Mailbox management** — add, edit, or remove mailbox mappings via a modal
  dialog.
- **Migration control** — start migrations, dry runs, repairs, fix drafts, and
  folder merges with a single click.  Every action shows a confirmation dialog
  explaining what it does before running.  Destructive actions are clearly
  flagged.  Real-time progress bar and live event stream via WebSocket.
- **Logs** — view the full migration log file and live log events.
- **History** — browse all past migration runs with per-mailbox statistics.

### Running locally

```bash
# From the MailboxMigration directory (virtual env activated):
pip install -r web/requirements.txt
python -m web.app
# Open http://localhost:8000 in your browser
```

Override the port with `PORT=8080 python -m web.app`.

### Running with Docker

```bash
# Build and start:
docker compose up --build -d

# Open http://localhost:8000
```

Mount your `config.yaml` into the container (see `docker-compose.yml`).

### Running on Kubernetes

Pre-built manifests are provided in the `k8s/` directory. This works on any
Kubernetes cluster, including ARM-based setups like a Raspberry Pi cluster
running k3s.

```bash
# 1. Create the namespace
kubectl apply -f k8s/namespace.yaml

# 2. Edit the secret with your real credentials
cp k8s/secret.example.yaml k8s/secret.yaml
# Edit k8s/secret.yaml with your Azure + IMAP credentials

# 3. Edit the ConfigMap with your IMAP host and mailbox mappings
#    (secrets are injected via env vars, not the config file)
kubectl apply -f k8s/configmap.yaml

# 4. Apply everything
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/pvc.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml

# 5. (Optional) Apply the ingress if you have an ingress controller
#    Edit k8s/ingress.yaml with your hostname first
kubectl apply -f k8s/ingress.yaml

# 6. Or port-forward to access locally
kubectl port-forward -n mail-migration svc/mail-migration 8000:80
# Open http://localhost:8000
```

The deployment uses `Recreate` strategy (SQLite doesn't support concurrent
writers), requests minimal resources (128 Mi RAM, 100m CPU), and stores state
on a 1 Gi PersistentVolumeClaim so migrations survive pod restarts.

### Cloud deployment

The Docker image runs anywhere that supports containers (Azure Container
Instances, AWS ECS, Google Cloud Run, a simple VPS, etc.).  For production use:

1. Set secrets via environment variables rather than baking them into
   `config.yaml`.
2. Mount a persistent volume for `migration_state.db` and logs.
3. Put a reverse proxy (Nginx / Caddy / cloud load balancer) in front for TLS.

---

## Project Structure

```
MailboxMigration/
├── migrate.py              # CLI entry point
├── config.example.yaml     # Example configuration
├── requirements.txt        # Python dependencies (CLI)
├── Dockerfile              # Docker image for web app
├── docker-compose.yml      # Docker Compose for easy deployment
├── README.md               # This file
├── k8s/                    # Kubernetes manifests
│   ├── namespace.yaml
│   ├── secret.example.yaml # Template — copy to secret.yaml and fill in
│   ├── configmap.yaml      # config.yaml as a ConfigMap
│   ├── pvc.yaml            # Persistent storage for state DB + logs
│   ├── deployment.yaml     # Pod spec with health checks
│   ├── service.yaml        # ClusterIP service
│   └── ingress.yaml        # Optional ingress (Traefik / Nginx)
├── src/                    # Core migration engine
│   ├── __init__.py
│   ├── config.py           # YAML config loader + validation
│   ├── logger.py           # Logging setup (file + rich console)
│   ├── state.py            # SQLite state DB for resume support
│   ├── imap_source.py      # IMAP connection, folder listing, message fetch
│   ├── graph_client.py     # Microsoft Graph API client (MSAL auth)
│   ├── folder_mapper.py    # Map IMAP folders to Exchange Online folders
│   └── mail_migrator.py    # Migration orchestrator
└── web/                    # Web GUI (FastAPI + Alpine.js)
    ├── __init__.py
    ├── __main__.py         # python -m web.app entry point
    ├── app.py              # FastAPI application + API routes
    ├── models.py           # Pydantic request/response models
    ├── runner.py           # Background job runner with WebSocket broadcasting
    ├── requirements.txt    # Additional Python dependencies (web only)
    ├── templates/
    │   └── index.html      # Single-page frontend (Alpine.js + Tailwind CSS)
    └── static/
        └── favicon.svg
```

---

## License

This tool is provided as-is for internal use. No warranty is implied.
