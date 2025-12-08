#!/usr/bin/env python3
"""
Local Test Script for Scheduled Alerts

Tests the alert pipeline locally using your Snowflake connection.
Proves the code works and will be ready when SPCS auth is supported.

Usage:
    # Activate the conda environment first
    conda activate snowflake_agents

    # Run from the scheduled_alerts directory
    python test_local.py

    # Or with explicit connection
    python test_local.py --connection snowflake_agents
"""

import argparse
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Test scheduled alerts locally")
    parser.add_argument(
        "--connection",
        "-c",
        default="snowflake_agents",
        help="Snowflake connection name from config.toml",
    )
    parser.add_argument(
        "--email",
        "-e",
        default="jeremy.demlow@snowflake.com",
        help="Email address for test alert",
    )
    parser.add_argument(
        "--question",
        "-q",
        default="What is total revenue by business unit (tickets, rentals, F&B) for December 1-3, 2025?",
        help="Question to ask the agent",
    )
    parser.add_argument(
        "--process-all",
        action="store_true",
        help="Process all active alerts instead of test",
    )
    parser.add_argument(
        "--send",
        action="store_true",
        help="Actually send the email (otherwise just test agent)",
    )
    args = parser.parse_args()

    from snowflake.snowpark import Session
    from scheduled_alerts.agent_client import AgentClient
    from scheduled_alerts.email_formatter import EmailFormatter
    from scheduled_alerts.alert_service import AlertService, AlertConfig

    logger.info(f"Creating Snowflake session with connection: {args.connection}")

    session = Session.builder.config("connection_name", args.connection).create()
    logger.info("Session created successfully")

    result = session.sql("SELECT CURRENT_USER(), CURRENT_ROLE()").collect()
    logger.info(f"Connected as: {result[0][0]} with role: {result[0][1]}")

    try:
        if args.process_all:
            logger.info("Processing all active alerts...")
            service = AlertService.from_session(session)
            results = service.process_all_alerts()

            print("\n" + "=" * 60)
            print("RESULTS")
            print("=" * 60)
            for r in results:
                status_emoji = (
                    "✅"
                    if r.status == "success"
                    else "❌"
                    if r.status == "error"
                    else "⚠️"
                )
                print(f"{status_emoji} Alert {r.alert_id}: {r.status}")
                if r.error:
                    print(f"   Error: {r.error}")
                if r.response_length:
                    print(f"   Response: {r.response_length} chars, {r.duration_ms}ms")

        else:
            logger.info(f"Testing alert to {args.email}")
            logger.info(f"Question: {args.question[:80]}...")

            # Test agent client
            logger.info("\n--- Testing Agent Client ---")

            config = AlertConfig()
            agent = AgentClient.from_session(
                session,
                agent_name=config.agent_name,
                database=config.database,
                schema=config.schema,
            )

            logger.info(f"Agent endpoint: {agent.config.endpoint}")
            logger.info("Calling agent...")

            response = agent.ask_detailed(args.question)

            logger.info(f"Response length: {len(response.text)} chars")
            logger.info(f"Duration: {response.duration_ms / 1000:.1f}s")
            logger.info(f"Tool calls: {len(response.tool_calls)}")

            print("\n" + "=" * 60)
            print("AGENT RESPONSE")
            print("=" * 60)
            print(response.text[:2000])
            if len(response.text) > 2000:
                print(f"\n... (truncated, {len(response.text)} total chars)")

            # Test email formatting
            logger.info("\n--- Testing Email Formatter ---")
            formatter = EmailFormatter()
            html = formatter.format(args.question, response.text)
            logger.info(f"Generated HTML: {len(html)} chars")

            if args.send:
                logger.info("\n--- Sending Email ---")
                service = AlertService.from_session(session)
                result = service.send_test_alert(args.email, args.question)

                print("\n" + "=" * 60)
                print("EMAIL RESULT")
                print("=" * 60)
                status_emoji = "✅" if result.status == "success" else "❌"
                print(f"{status_emoji} Status: {result.status}")
                if result.error:
                    print(f"Error: {result.error}")
                else:
                    print(f"Response: {result.response_length} chars")
                    print(f"Duration: {result.duration_ms / 1000:.1f}s")
                    print(f"\n📧 Check {args.email} for the test email!")
            else:
                logger.info("\nTo send email, run with --send flag")

    finally:
        session.close()
        logger.info("Session closed")


if __name__ == "__main__":
    main()
