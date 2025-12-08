# Scheduled Alerts

Production-ready module for sending scheduled Cortex Agent insights via email.

## Architecture

```
scheduled_alerts/
├── agent_client.py      # Cortex Agent REST API client
├── email_formatter.py   # HTML email formatting with markdown/premailer
├── alert_service.py     # Alert orchestration service
├── test_local.py        # Local testing script
└── job_service/
    ├── alert_job.py     # SPCS compute pool job
    └── alert_job_spec.yaml
```

## Authentication Methods

The `AgentClient` supports multiple authentication methods:

### 1. Snowpark Session (Local Development)
```python
from snowflake.snowpark import Session
from scheduled_alerts.agent_client import AgentClient

session = Session.builder.config("connection_name", "snowflake_agents").create()
client = AgentClient.from_session(session, agent_name="RESORT_EXECUTIVE_DEV")
```

### 2. Personal Access Token (PAT) - Simplest for External Systems
```python
from scheduled_alerts.agent_client import AgentClient

client = AgentClient.from_pat(
    agent_name="RESORT_EXECUTIVE_DEV",
    database="SKI_RESORT_DB",
    schema="AGENTS",
    account="xy12345.us-west-2",
    pat="your_personal_access_token"
)
```

Generate PATs in Snowsight: User Menu > My Profile > Personal Access Tokens

### 3. Key-Pair (External Schedulers like Airflow)
```python
from scheduled_alerts.agent_client import AgentClient

client = AgentClient.from_key_pair(
    agent_name="RESORT_EXECUTIVE_DEV",
    database="SKI_RESORT_DB",
    schema="AGENTS",
    account="xy12345",
    user="MY_USER",
    private_key_path="/path/to/rsa_key.p8"
)
```

### 4. Container Token (SPCS/ML Jobs)
```python
from scheduled_alerts.agent_client import AgentClient

# When running on compute pool
client = AgentClient.from_container(
    agent_name="RESORT_EXECUTIVE_DEV",
    database="SKI_RESORT_DB",
    schema="AGENTS"
)
```

**Note:** As of Dec 2024, the SPCS container token is not yet supported for Agent REST API calls. This will work when Snowflake enables it.

## Usage

### Testing Locally

```bash
# Activate conda environment
conda activate snowflake_agents

# Test agent (without sending email)
cd agent_tools/src/scheduled_alerts
python test_local.py

# Test and send email
python test_local.py --send

# Custom question
python test_local.py -q "What is revenue by business unit?" --send

# Process all active alerts
python test_local.py --process-all
```

### Using AlertService

```python
from snowflake.snowpark import Session
from scheduled_alerts.alert_service import AlertService

session = Session.builder.config("connection_name", "snowflake_agents").create()
service = AlertService.from_session(session)

# Send test alert
result = service.send_test_alert("user@example.com", "What is total revenue?")

# Process all scheduled alerts
results = service.process_all_alerts()
```

## Email Formatting

Emails are formatted using:
1. `markdown` - Converts agent markdown to HTML
2. `premailer` - Inlines CSS for email client compatibility

The email includes:
- Professional header with question and timestamp
- Formatted response with tables, lists, code blocks
- Branded footer

## Dependencies

```
requests
markdown
premailer
snowflake-snowpark-python
```

For key-pair auth:
```
cryptography
PyJWT
```
