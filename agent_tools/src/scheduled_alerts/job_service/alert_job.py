#!/usr/bin/env python3
"""
Alert Job - Runs on SPCS compute pool.

This script processes scheduled alerts when run on a compute pool.
Uses the container token for authentication (when supported by Snowflake).

Usage:
    # Process all alerts
    python alert_job.py

    # Test single alert
    python alert_job.py "user@example.com" "What is total revenue?"

Note: As of Dec 2024, the container OAuth token is NOT valid for calling
the Cortex Agent REST API. This will work when Snowflake enables it.
"""

import json
import os
import sys
from datetime import datetime

import markdown
import requests
import snowflake.connector

# Configuration
DATABASE = "SKI_RESORT_DB"
SCHEMA = "AGENTS"
AGENT_NAME = "RESORT_EXECUTIVE_DEV"
EMAIL_INTEGRATION = "MY_EMAIL_INT"
ALERTS_TABLE = f"{DATABASE}.{SCHEMA}.SCHEDULED_ALERTS"


def get_snowflake_connection():
    """Create Snowflake connection using SPCS token."""
    token_path = "/snowflake/session/token"

    if not os.path.exists(token_path):
        raise RuntimeError("Token file not found - not running on compute pool")

    with open(token_path, "r") as f:
        token = f.read().strip()

    host = os.getenv("SNOWFLAKE_HOST")
    account = os.getenv("SNOWFLAKE_ACCOUNT")

    print(f"ENV SNOWFLAKE_HOST: {host}")
    print(f"ENV SNOWFLAKE_ACCOUNT: {account}")

    conn = snowflake.connector.connect(
        host=host, account=account, token=token, authenticator="oauth"
    )

    # Get actual host from connection for REST API calls
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_ACCOUNT(), CURRENT_REGION()")
    row = cursor.fetchone()
    actual_account = row[0].lower()
    region = row[1]

    # Parse region to construct REST API host
    region_parts = region.split(".")
    if len(region_parts) >= 2:
        cloud_region = region_parts[-1].lower().replace("_", "-")
        if cloud_region.startswith("aws-"):
            cloud_region = cloud_region[4:]
        rest_host = f"{actual_account}.{cloud_region}.snowflakecomputing.com"
    else:
        rest_host = host

    print(f"REST API Host: {rest_host}")
    cursor.close()

    return conn, token, rest_host


def call_agent(host: str, token: str, question: str) -> str:
    """
    Call the Cortex Agent REST API.

    Note: As of Dec 2024, the SPCS OAuth token is NOT valid for REST API calls.
    This will return a 401 error until Snowflake enables it.
    """
    url = f"https://{host}/api/v2/databases/{DATABASE}/schemas/{SCHEMA}/agents/{AGENT_NAME}:run"

    print(f"Host: {host}")
    print(f"Token length: {len(token)}")

    headers = {
        "Authorization": f'Snowflake Token="{token}"',
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
    }

    body = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": question}]}]
    }

    print(f"Calling agent: {question[:80]}...")
    response = requests.post(url, headers=headers, json=body, stream=True, timeout=300)

    if response.status_code == 401:
        raise RuntimeError(
            f"Authentication failed (401). The SPCS OAuth token is not yet supported "
            f"for Cortex Agent REST API calls. This is a known gap as of Dec 2024. "
            f"Response: {response.text[:200]}"
        )

    if response.status_code != 200:
        raise RuntimeError(
            f"Agent API error: {response.status_code} - {response.text[:500]}"
        )

    # Parse SSE stream - only get response.text events
    final_text = ""
    current_event = None

    for line in response.iter_lines(decode_unicode=True):
        if not line:
            continue

        if line.startswith("event:"):
            current_event = line[6:].strip()
            continue

        if not line.startswith("data:"):
            continue

        data = line[5:].strip()
        if data == "[DONE]":
            break

        try:
            event = json.loads(data)

            if current_event == "response.text.delta":
                if "text" in event:
                    final_text += event["text"]

            elif current_event == "response":
                if "content" in event:
                    for item in event["content"]:
                        if item.get("type") == "text":
                            final_text = item.get("text", "")

        except json.JSONDecodeError:
            continue

    print(f"Got response: {len(final_text)} chars")
    return final_text.strip()


def format_email(question: str, response: str) -> str:
    """Format agent response as HTML email."""
    timestamp = datetime.now().strftime("%B %d, %Y at %I:%M %p")
    response_html = markdown.markdown(
        response, extensions=["tables", "fenced_code", "nl2br", "sane_lists"]
    )

    title = question[:80] + "..." if len(question) > 80 else question

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 0; background: #f6f9fc;">
<div style="max-width: 700px; margin: 0 auto; background: #ffffff; border-radius: 8px; overflow: hidden;">
    <div style="background: linear-gradient(135deg, #1a73e8, #0d47a1); color: #fff; padding: 30px 40px;">
        <h1 style="font-size: 20px; margin: 0 0 10px 0; color: white;">🎿 {title}</h1>
        <div style="font-size: 13px; opacity: 0.9;">Generated by Snowflake Intelligence | {timestamp}</div>
    </div>
    <div style="padding: 30px 40px; color: #1f2937; line-height: 1.7; font-size: 14px;">
        {response_html}
    </div>
    <div style="background: #f8f9fa; padding: 20px 40px; font-size: 12px; color: #666; border-top: 1px solid #e0e0e0;">
        <strong>Ski Resort Analytics</strong> | Powered by Snowflake Cortex
    </div>
</div>
</body>
</html>"""


def send_email(cursor, recipient: str, subject: str, html: str):
    """Send email via Snowflake."""
    escaped_html = html.replace("'", "''")
    cursor.execute(
        f"""
        CALL SYSTEM$SEND_EMAIL(
            '{EMAIL_INTEGRATION}',
            '{recipient}',
            '{subject}',
            '{escaped_html}',
            'text/html'
        )
    """
    )
    print(f"Email sent to {recipient}")


def process_alerts():
    """Main function to process all active alerts."""
    print("Starting alert processing...")

    conn, token, host = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            f"""
            SELECT ALERT_ID, USER_EMAIL, OVERALL_QUESTION, ALERT_FREQUENCY
            FROM {ALERTS_TABLE}
            WHERE IS_ACTIVE = TRUE
        """
        )

        alerts = cursor.fetchall()
        print(f"Found {len(alerts)} active alerts")

        results = []

        for alert in alerts:
            alert_id, email, question, frequency = alert

            try:
                print(f"\nProcessing alert {alert_id} for {email}")

                response = call_agent(host, token, question)

                if not response or len(response) < 50:
                    print("Short response, skipping")
                    results.append({"alert_id": alert_id, "status": "skipped"})
                    continue

                html = format_email(question, response)
                subject = f"🎿 {question[:50]}..."
                send_email(cursor, email, subject, html)

                cursor.execute(
                    f"""
                    UPDATE {ALERTS_TABLE}
                    SET LAST_SENT_AT = CURRENT_TIMESTAMP(),
                        SEND_COUNT = SEND_COUNT + 1
                    WHERE ALERT_ID = {alert_id}
                """
                )

                results.append({"alert_id": alert_id, "status": "success"})

            except Exception as e:
                print(f"Error processing alert {alert_id}: {e}")
                results.append(
                    {"alert_id": alert_id, "status": "error", "error": str(e)}
                )

        print(f"\nCompleted: {len(results)} alerts processed")
        return results

    finally:
        cursor.close()
        conn.close()


def test_single_alert(email: str, question: str):
    """Test sending a single alert."""
    print(f"Testing alert for {email}")

    conn, token, host = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        response = call_agent(host, token, question)

        if response and len(response) >= 50:
            html = format_email(question, response)
            subject = f"🎿 [TEST] {question[:50]}..."
            send_email(cursor, email, subject, html)
            print("Test alert sent successfully!")
        else:
            print(f"Response too short: {len(response)} chars")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) >= 3:
        test_single_alert(sys.argv[1], sys.argv[2])
    else:
        process_alerts()
