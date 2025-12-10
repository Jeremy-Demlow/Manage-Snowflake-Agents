-- ============================================================================
-- SCHEDULED ALERTS SYSTEM SETUP
-- ============================================================================
-- Complete setup for automated agent-powered alerts with email delivery
-- Uses PAT-as-Secret pattern for reliable agent API calls
--
-- Prerequisites:
-- 1. Run as ACCOUNTADMIN
-- 2. Have a PAT token generated from Snowflake UI
-- 3. Email integration (ai_email_int) already configured
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE SKI_RESORT_DB;
USE SCHEMA MODELS;

-- ============================================================================
-- STEP 1: NETWORK RULE FOR SNOWFLAKE API EGRESS
-- ============================================================================
CREATE OR REPLACE NETWORK RULE CORTEX_AGENT_EGRESS_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('trb65519.snowflakecomputing.com');

-- ============================================================================
-- STEP 2: SECRET FOR PAT TOKEN
-- ============================================================================
-- Generate PAT: Snowflake UI → User Menu → My Profile → Authentication → Generate Token
-- Update the SECRET_STRING with your actual PAT
CREATE OR REPLACE SECRET CORTEX_AGENT_PAT_SECRET
  TYPE = GENERIC_STRING
  SECRET_STRING = '<YOUR_PAT_TOKEN_HERE>';

-- ============================================================================
-- STEP 3: EXTERNAL ACCESS INTEGRATION
-- ============================================================================
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION CORTEX_AGENT_ACCESS_INTEGRATION
  ALLOWED_NETWORK_RULES = (CORTEX_AGENT_EGRESS_RULE)
  ALLOWED_AUTHENTICATION_SECRETS = (CORTEX_AGENT_PAT_SECRET)
  ENABLED = TRUE;

-- Grant to role
GRANT USAGE ON INTEGRATION CORTEX_AGENT_ACCESS_INTEGRATION TO ROLE ACCOUNTADMIN;

-- ============================================================================
-- STEP 4: GRANT PYPI REPOSITORY ACCESS (for premailer package)
-- ============================================================================
GRANT DATABASE ROLE SNOWFLAKE.PYPI_REPOSITORY_USER TO ROLE ACCOUNTADMIN;

-- ============================================================================
-- STEP 5: AGENT CALLER UDF
-- ============================================================================
-- Calls the Cortex Agent via REST API using PAT authentication
CREATE OR REPLACE FUNCTION ASK_RESORT_EXECUTIVE(user_query VARCHAR)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests', 'snowflake-snowpark-python')
EXTERNAL_ACCESS_INTEGRATIONS = (CORTEX_AGENT_ACCESS_INTEGRATION)
SECRETS = ('agent_token' = CORTEX_AGENT_PAT_SECRET)
HANDLER = 'run_agent'
AS
$$
import _snowflake
import requests
import json

def run_agent(user_query):
    try:
        token = _snowflake.get_generic_secret_string('agent_token')
    except Exception as e:
        return f'Error reading secret: {str(e)}'

    url = 'https://trb65519.snowflakecomputing.com/api/v2/databases/SKI_RESORT_DB/schemas/AGENTS/agents/RESORT_EXECUTIVE_DEV:run'

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'Accept': 'text/event-stream'
    }

    payload = {
        'messages': [
            {'role': 'user', 'content': [{'type': 'text', 'text': user_query}]}
        ]
    }

    try:
        response = requests.post(url, headers=headers, json=payload, stream=True, timeout=300)

        if response.status_code != 200:
            return f'API Error {response.status_code}: {response.text[:500]}'

        final_answer = []
        current_event = None

        for line in response.iter_lines():
            if not line:
                continue
            decoded = line.decode('utf-8')

            if decoded.startswith('event: '):
                current_event = decoded[7:].strip()

            if decoded.startswith('data: '):
                data_str = decoded[6:]
                if data_str == '[DONE]':
                    break
                try:
                    data = json.loads(data_str)
                    if current_event == 'response.text.delta' and 'text' in data:
                        final_answer.append(data['text'])
                except:
                    continue

        return ''.join(final_answer) if final_answer else 'Agent returned no content.'

    except Exception as e:
        return f'Connection error: {str(e)}'
$$;

-- ============================================================================
-- STEP 6: EMAIL FORMATTER UDF (with PyPI packages)
-- ============================================================================
-- Uses Artifact Repository to access premailer from PyPI
CREATE OR REPLACE FUNCTION FORMAT_ALERT_EMAIL(question VARCHAR, response VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('markdown', 'premailer')
HANDLER = 'format_email'
AS
$$
import markdown
from premailer import Premailer
from datetime import datetime

def format_email(question, response):
    # Convert markdown to HTML
    md = markdown.Markdown(extensions=['tables', 'fenced_code'])
    content_html = md.convert(response)

    # Build full HTML with styling
    html = f'''<!DOCTYPE html>
<html>
<head>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; max-width: 800px; margin: 0 auto; padding: 20px; }}
.header {{ background: linear-gradient(135deg, #1a73e8, #0d47a1); color: white; padding: 24px; border-radius: 12px 12px 0 0; }}
.header h1 {{ margin: 0; font-size: 18px; }}
.header .meta {{ opacity: 0.9; font-size: 12px; margin-top: 8px; }}
.content {{ background: #fff; padding: 24px; border: 1px solid #e0e0e0; border-top: none; border-radius: 0 0 12px 12px; }}
table {{ border-collapse: collapse; width: 100%; margin: 16px 0; }}
th {{ background: #f5f5f5; padding: 12px; text-align: left; border-bottom: 2px solid #1a73e8; }}
td {{ padding: 12px; border-bottom: 1px solid #eee; }}
h2 {{ color: #1a73e8; border-bottom: 2px solid #1a73e8; padding-bottom: 8px; }}
h3 {{ color: #333; }}
.footer {{ text-align: center; padding: 16px; color: #666; font-size: 12px; }}
</style>
</head>
<body>
<div class="header">
    <h1>🎿 {question[:80]}...</h1>
    <div class="meta">Generated by Snowflake Intelligence | {datetime.now().strftime("%B %d, %Y at %I:%M %p")}</div>
</div>
<div class="content">
{content_html}
</div>
<div class="footer">
    <strong>Ski Resort Analytics</strong> | Powered by Snowflake Cortex
</div>
</body>
</html>'''

    # Inline CSS for email compatibility
    try:
        premailer = Premailer(html, remove_classes=False)
        return premailer.transform()
    except:
        return html
$$;

-- ============================================================================
-- STEP 7: ALERTS TABLES
-- ============================================================================
CREATE TABLE IF NOT EXISTS SCHEDULED_ALERTS (
    alert_id VARCHAR DEFAULT UUID_STRING(),
    alert_name VARCHAR NOT NULL,
    recipient_email VARCHAR NOT NULL,
    question VARCHAR NOT NULL,
    frequency VARCHAR DEFAULT 'daily',  -- daily, weekly, monthly
    is_active BOOLEAN DEFAULT TRUE,
    last_sent_at TIMESTAMP_NTZ,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (alert_id)
);

CREATE TABLE IF NOT EXISTS ALERT_LOG (
    log_id VARCHAR DEFAULT UUID_STRING(),
    alert_id VARCHAR,
    sent_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    question VARCHAR,
    response VARCHAR,
    status VARCHAR,  -- success, failed
    error_message VARCHAR,
    PRIMARY KEY (log_id)
);

-- ============================================================================
-- STEP 8: ALERT PROCESSING PROCEDURE
-- ============================================================================
CREATE OR REPLACE PROCEDURE PROCESS_SCHEDULED_ALERTS()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'process_alerts'
AS
$$
def process_alerts(session):
    # Get pending alerts
    alerts_df = session.sql('''
        SELECT alert_id, alert_name, recipient_email, question
        FROM SCHEDULED_ALERTS
        WHERE is_active = TRUE
          AND frequency = 'daily'
          AND (last_sent_at IS NULL OR last_sent_at < CURRENT_DATE())
    ''').collect()

    processed = 0

    for row in alerts_df:
        alert_id = row['ALERT_ID']
        alert_name = row['ALERT_NAME']
        recipient = row['RECIPIENT_EMAIL']
        question = row['QUESTION']

        try:
            # Call agent via UDF
            response_df = session.sql(f"""
                SELECT ASK_RESORT_EXECUTIVE('{question.replace("'", "''")}') as response
            """).collect()
            response = response_df[0]['RESPONSE'] if response_df else 'No response'

            # Format email using formatter UDF
            html_df = session.sql(f"""
                SELECT FORMAT_ALERT_EMAIL(
                    '{question.replace("'", "''")}',
                    '{response.replace("'", "''")}'
                ) as html
            """).collect()
            html_body = html_df[0]['HTML'] if html_df else response

            subject = f'Ski Resort Alert: {alert_name}'

            # Send email
            session.call('SYSTEM$SEND_EMAIL', 'ai_email_int', recipient, subject, html_body, 'text/html')

            # Log success
            session.sql(f"""
                INSERT INTO ALERT_LOG (alert_id, question, response, status)
                VALUES ('{alert_id}', '{question.replace("'", "''")}', '{response.replace("'", "''")[:4000]}', 'success')
            """).collect()

            # Update last_sent
            session.sql(f"""
                UPDATE SCHEDULED_ALERTS SET last_sent_at = CURRENT_TIMESTAMP() WHERE alert_id = '{alert_id}'
            """).collect()

            processed += 1

        except Exception as e:
            session.sql(f"""
                INSERT INTO ALERT_LOG (alert_id, question, status, error_message)
                VALUES ('{alert_id}', '{question.replace("'", "''")}', 'failed', '{str(e)[:500].replace("'", "''")}')
            """).collect()

    return f'Processed: {processed} alerts'
$$;

-- ============================================================================
-- STEP 9: SCHEDULED TASK (8am PST daily)
-- ============================================================================
CREATE OR REPLACE TASK DAILY_ALERTS_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 8 * * * America/Los_Angeles'
AS
    CALL PROCESS_SCHEDULED_ALERTS();

-- Enable the task
ALTER TASK DAILY_ALERTS_TASK RESUME;

-- ============================================================================
-- STEP 10: SAMPLE ALERT (optional)
-- ============================================================================
-- INSERT INTO SCHEDULED_ALERTS (alert_name, recipient_email, question, frequency)
-- VALUES (
--     'Daily Operations Summary',
--     'your-email@company.com',
--     'Give me a summary of operations for the last 3 days including visitor counts, revenue breakdown by category, and any notable trends.',
--     'daily'
-- );

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SELECT 'Scheduled Alerts System Setup Complete!' as status;
SHOW TASKS LIKE 'DAILY_ALERTS%';
SELECT * FROM SCHEDULED_ALERTS;
