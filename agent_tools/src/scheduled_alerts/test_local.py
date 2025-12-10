#!/usr/bin/env python3
"""
Local Test Script for Scheduled Alerts

Tests the agent caller and email formatter locally before deploying to Snowflake.

Usage:
    # Activate conda environment
    conda activate snowflake_agents

    # Run from the src directory
    cd agent_tools/src
    python -m scheduled_alerts.test_local

    # Or with options
    python -m scheduled_alerts.test_local --question "What is total revenue?"
    python -m scheduled_alerts.test_local --send --email you@example.com
"""

import argparse
import json
import os
import sys
from datetime import datetime

import requests


def get_pat_token() -> str:
    """Get PAT token from environment."""
    token = os.environ.get("SNOWFLAKE_PAT")
    if not token:
        raise ValueError(
            "SNOWFLAKE_PAT environment variable not set.\n"
            "Set it with: export SNOWFLAKE_PAT='your-token-here'"
        )
    return token


def call_agent(question: str, token: str) -> str:
    """Call the Cortex Agent via REST API using PAT."""
    url = "https://trb65519.snowflakecomputing.com/api/v2/databases/SKI_RESORT_DB/schemas/AGENTS/agents/RESORT_EXECUTIVE_DEV:run"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
    }

    payload = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": question}]}]
    }

    print(f"📡 Calling agent...")
    start = datetime.now()

    response = requests.post(
        url, headers=headers, json=payload, stream=True, timeout=300
    )

    if response.status_code != 200:
        raise RuntimeError(f"Agent error {response.status_code}: {response.text[:500]}")

    # Parse SSE stream
    final_text = []
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
            if current_event == "response.text.delta" and "text" in event:
                final_text.append(event["text"])
        except json.JSONDecodeError:
            continue

    duration = (datetime.now() - start).total_seconds()
    result = "".join(final_text).strip()
    print(f"✅ Got response: {len(result)} chars in {duration:.1f}s")

    return result


def format_email_local(question: str, response: str) -> str:
    """Format email using the same logic as the UDF."""
    # Import the actual formatter
    from scheduled_alerts.email_formatter import format_email

    return format_email(question, response)


def send_via_snowflake(email: str, subject: str, html: str):
    """Send email via Snowflake (requires snowpark session)."""
    from snowflake.snowpark import Session

    session = Session.builder.config("connection_name", "snowflake_agents").create()

    escaped_html = html.replace("'", "''")
    escaped_subject = subject.replace("'", "''")

    session.sql(
        f"""
        CALL SYSTEM$SEND_EMAIL(
            'ai_email_int',
            '{email}',
            '{escaped_subject}',
            '{escaped_html}',
            'text/html'
        )
    """
    ).collect()

    session.close()
    print(f"📧 Email sent to {email}")


def main():
    parser = argparse.ArgumentParser(description="Test scheduled alerts locally")
    parser.add_argument(
        "--question",
        "-q",
        default="What is total revenue by business unit for December 1-3, 2025?",
        help="Question to ask the agent",
    )
    parser.add_argument(
        "--email",
        "-e",
        default="jeremy.demlow@snowflake.com",
        help="Email address for test",
    )
    parser.add_argument(
        "--send", action="store_true", help="Actually send the email via Snowflake"
    )
    parser.add_argument(
        "--save-html", type=str, help="Save HTML to file for inspection"
    )
    args = parser.parse_args()

    print("=" * 60)
    print("🎿 Scheduled Alerts - Local Test")
    print("=" * 60)
    print(f"Question: {args.question[:60]}...")
    print()

    # Get token
    try:
        token = get_pat_token()
        print(f"✅ PAT token loaded ({len(token)} chars)")
    except ValueError as e:
        print(f"❌ {e}")
        sys.exit(1)

    # Call agent
    try:
        response = call_agent(args.question, token)
    except Exception as e:
        print(f"❌ Agent error: {e}")
        sys.exit(1)

    # Show response preview
    print()
    print("=" * 60)
    print("AGENT RESPONSE")
    print("=" * 60)
    print(response[:1000])
    if len(response) > 1000:
        print(f"\n... (truncated, {len(response)} total chars)")

    # Format email
    print()
    print("=" * 60)
    print("FORMATTING EMAIL")
    print("=" * 60)
    html = format_email_local(args.question, response)
    print(f"✅ Generated HTML: {len(html)} chars")

    # Save HTML if requested
    if args.save_html:
        with open(args.save_html, "w") as f:
            f.write(html)
        print(f"💾 Saved to {args.save_html}")
        print(f"   Open in browser: file://{os.path.abspath(args.save_html)}")

    # Send email if requested
    if args.send:
        print()
        print("=" * 60)
        print("SENDING EMAIL")
        print("=" * 60)
        subject = f"🎿 [TEST] {args.question[:50]}..."
        try:
            send_via_snowflake(args.email, subject, html)
        except Exception as e:
            print(f"❌ Send error: {e}")
            sys.exit(1)
    else:
        print()
        print("💡 To send email, run with --send flag")
        print("💡 To save HTML, run with --save-html output.html")

    print()
    print("✅ Test complete!")


if __name__ == "__main__":
    main()
