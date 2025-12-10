# Scheduled Alerts

Snowflake UDFs and procedures for automated alert emails.

## Files

| File | Purpose | Snowflake Object |
|------|---------|------------------|
| `agent_caller.py` | Call Cortex Agent via PAT | `ASK_RESORT_EXECUTIVE(question)` |
| `email_formatter.py` | Markdown → styled HTML | `FORMAT_ALERT_EMAIL(question, response)` |
| `alerts_processor.py` | Batch process alerts | `PROCESS_SCHEDULED_ALERTS()` |
| `test_local.py` | Local development testing | - |

## Local Testing

```bash
# Set your PAT token
export SNOWFLAKE_PAT='your-token-here'

# Activate environment
conda activate snowflake_agents

# Run from src directory  
cd agent_tools/src
python -m scheduled_alerts.test_local

# Save HTML to inspect formatting
python -m scheduled_alerts.test_local --save-html output.html

# Send test email via Snowflake
python -m scheduled_alerts.test_local --send --email you@example.com
```

## Deploy

```bash
cd agent_tools
rm src.zip && zip -r src.zip src/
snow sql -q "PUT file://src.zip @SKI_RESORT_DB.MODELS.AGENT_TOOLS_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
```

Then run the CREATE statements from `sql/scheduled_alerts_setup.sql`.

## Test in Snowflake

```sql
-- Test agent UDF
SELECT SKI_RESORT_DB.MODELS.ASK_RESORT_EXECUTIVE('What is total revenue?');

-- Test email formatter  
SELECT SKI_RESORT_DB.MODELS.FORMAT_ALERT_EMAIL('Test question', 'Test **response**');

-- Send test email
CALL SKI_RESORT_DB.MODELS.TEST_ALERT_EMAIL('you@example.com', 'What is total revenue?');
```
