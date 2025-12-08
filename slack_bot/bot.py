#!/usr/bin/env python3
"""
Snowflake Intelligence Slack Bot

A clean, modular Slack bot for querying Snowflake Cortex Agents.

Features:
- Multi-turn conversations with context
- Progress updates (non-spammy)
- Data tables and SQL display
- Chart rendering (Vega-Lite to PNG)
- CSV data export
- Configurable via config.yml

Usage:
    # Set environment variables
    export SNOWFLAKE_ACCOUNT="your-account"
    export SNOWFLAKE_PAT="your-pat-token"
    export SLACK_BOT_TOKEN="xoxb-..."
    export SLACK_APP_TOKEN="xapp-..."

    # Run the bot
    python bot.py

    # Or test without Slack
    python bot.py --test "How many customers do we have?"
"""

import argparse
import logging
import os
import re
import time
from typing import Optional

import yaml

from agent import AgentClient, AgentResult
from context import ConversationContext, get_context
from formatters import ChartRenderer, SlackFormatter

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ─── Configuration ────────────────────────────────────────────────────────────


def load_config(config_path: str = "config.yml") -> dict:
    """Load configuration from YAML file."""
    if not os.path.exists(config_path):
        logger.warning(f"Config file {config_path} not found, using defaults")
        return get_default_config()

    with open(config_path) as f:
        return yaml.safe_load(f)


def get_default_config() -> dict:
    """Default configuration."""
    return {
        "defaults": {
            "timeout": 120,
            "max_context_messages": 10,
            "context_ttl_hours": 1,
        },
        "agents": {
            "default": {
                "name": os.getenv("AGENT_NAME", "RESORT_EXECUTIVE_DEV"),
                "database": os.getenv("AGENT_DATABASE", "SKI_RESORT_DB"),
                "schema": os.getenv("AGENT_SCHEMA", "AGENTS"),
                "emoji": "🎿",
            }
        },
        "slack": {
            "formatting": {
                "max_response_length": 2500,
                "max_table_rows": 15,
                "show_sql": True,
                "show_timing": True,
            }
        },
    }


# Load config at import time
CONFIG = load_config()


# ─── Clients ──────────────────────────────────────────────────────────────────


def get_agent_client(agent_key: str = "default") -> AgentClient:
    """Get an agent client for the specified agent."""
    agent_config = CONFIG.get("agents", {}).get(agent_key)
    if not agent_config:
        agent_config = CONFIG.get("agents", {}).get("default", {})

    return AgentClient.from_config(
        agent_config,
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        pat=os.getenv("SNOWFLAKE_PAT"),
    )


def get_formatter() -> SlackFormatter:
    """Get the Slack formatter with config settings."""
    fmt_config = CONFIG.get("slack", {}).get("formatting", {})
    return SlackFormatter(
        max_response_length=fmt_config.get("max_response_length", 2500),
        max_table_rows=fmt_config.get("max_table_rows", 15),
        show_sql=fmt_config.get("show_sql", True),
        show_timing=fmt_config.get("show_timing", True),
    )


# ─── Slack Integration ────────────────────────────────────────────────────────

# Import Slack only if available
try:
    from slack_bolt import App
    from slack_bolt.adapter.socket_mode import SocketModeHandler

    SLACK_AVAILABLE = True
except ImportError:
    SLACK_AVAILABLE = False
    logger.info("Slack Bolt not installed - run: pip install slack-bolt")


def create_slack_app() -> Optional["App"]:
    """Create and configure the Slack app."""
    if not SLACK_AVAILABLE:
        return None

    bot_token = os.getenv("SLACK_BOT_TOKEN")
    app_token = os.getenv("SLACK_APP_TOKEN")

    if not bot_token or not app_token:
        logger.warning("Slack tokens not configured")
        return None

    app = App(token=bot_token)

    # Get shared resources
    context = get_context(
        ttl_hours=CONFIG.get("defaults", {}).get("context_ttl_hours", 1),
        max_messages=CONFIG.get("defaults", {}).get("max_context_messages", 10),
    )
    formatter = get_formatter()
    chart_renderer = ChartRenderer()

    # Get bot user ID for mention detection
    bot_user_id = None
    try:
        bot_user_id = app.client.auth_test()["user_id"]
    except Exception as e:
        logger.warning(f"Could not get bot user ID: {e}")

    # ─── App Mention Handler ──────────────────────────────────────────────────

    @app.event("app_mention")
    def handle_app_mention(event, say):
        """Handle @mentions of the bot."""
        # Ignore bot messages
        if event.get("bot_id"):
            return

        question = event.get("text", "")
        channel = event.get("channel")
        thread_ts = event.get("thread_ts") or event.get("ts")

        # Clean up @mention from question
        if bot_user_id:
            question = question.replace(f"<@{bot_user_id}>", "").strip()

        if not question:
            return

        logger.info(f"@mention: '{question[:50]}...'")

        # Handle the question
        _handle_question(
            app, say, channel, thread_ts, question, context, formatter, chart_renderer
        )

    # ─── Message Handler ──────────────────────────────────────────────────────

    @app.message(re.compile(".*"))
    def handle_message(message, say):
        """Handle direct messages and thread replies."""
        # Ignore bot messages
        if message.get("bot_id"):
            return

        question = message["text"]
        channel_type = message.get("channel_type", "")
        thread_ts = message.get("thread_ts") or message.get("ts")
        is_in_thread = message.get("thread_ts") is not None

        # Only respond to: DMs or threads we're already in
        in_our_thread = is_in_thread and context.has_context(thread_ts)
        is_dm = channel_type == "im"

        # Skip if not DM and not in our thread (app_mention handles @mentions)
        if not (is_dm or in_our_thread):
            return

        # Clean up @mention from question (in case it's in thread)
        if bot_user_id:
            question = question.replace(f"<@{bot_user_id}>", "").strip()

        if not question:
            return

        logger.info(
            f"Message: '{question[:50]}...' (dm={is_dm}, thread={in_our_thread})"
        )

        # Handle the question
        _handle_question(
            app,
            say,
            message["channel"],
            thread_ts,
            question,
            context,
            formatter,
            chart_renderer,
        )

    # ─── Slash Command Handler ────────────────────────────────────────────────

    @app.command("/ask")
    def handle_ask_command(ack, command, say, respond):
        """Handle /ask slash command."""
        ack()  # Must ack immediately

        question = command.get("text", "").strip()
        user_id = command.get("user_id")
        channel_id = command.get("channel_id")

        if not question:
            respond("Usage: `/ask <your question>`")
            return

        logger.info(f"Slash command from {user_id}: '{question[:50]}...'")

        # Post question publicly
        agent_config = CONFIG.get("agents", {}).get("default", {})
        emoji = agent_config.get("emoji", "🎿")

        question_msg = say(
            blocks=formatter.format_question_header(question, user_id, emoji),
            text=f"<@{user_id}> asked: {question}",
            channel=channel_id,
        )
        thread_ts = question_msg["ts"]

        # Handle in thread
        _handle_question(
            app,
            say,
            channel_id,
            thread_ts,
            question,
            context,
            formatter,
            chart_renderer,
        )

    return app


def _handle_question(
    app: "App",
    say,
    channel: str,
    thread_ts: str,
    question: str,
    context: ConversationContext,
    formatter: SlackFormatter,
    chart_renderer: ChartRenderer,
):
    """Core question handling logic."""
    start_time = time.time()

    # Post initial progress
    progress_msg = say("🤔 Analyzing...", thread_ts=thread_ts)
    progress_ts = progress_msg["ts"]
    last_update = start_time

    # Progress callback - updates existing message
    def update_progress(status: str):
        nonlocal last_update
        now = time.time()

        # Update every 5s or on key milestones
        key_words = ["planning", "executing", "generating", "forming"]
        is_milestone = any(w in status.lower() for w in key_words)

        if is_milestone or (now - last_update >= 5.0):
            last_update = now
            try:
                app.client.chat_update(
                    channel=channel,
                    ts=progress_ts,
                    text=formatter.format_progress(status),
                )
            except Exception:
                pass

    try:
        # Get conversation history
        history = context.get_history(thread_ts)

        # Call agent
        client = get_agent_client()
        agent_config = CONFIG.get("agents", {}).get("default", {})
        emoji = agent_config.get("emoji", "🎿")

        result = client.ask(
            question, conversation_history=history, progress_callback=update_progress
        )

        # Update progress to complete
        elapsed = time.time() - start_time
        try:
            app.client.chat_update(
                channel=channel, ts=progress_ts, text=f"✅ Complete ({elapsed:.1f}s)"
            )
        except Exception:
            pass

        # Format and post response
        blocks = formatter.format_response(result, question, emoji)
        say(blocks=blocks, text=result.answer[:500], thread_ts=thread_ts)

        # Upload charts
        if result.chart_specs and chart_renderer.available:
            for idx, png in enumerate(chart_renderer.render_all(result)):
                try:
                    chart_type = result.chart_specs[idx].get("type", "chart")
                    app.client.files_upload_v2(
                        channel=channel,
                        file=png.getvalue(),
                        filename=f"chart_{idx+1}_{chart_type}.png",
                        title=f"📊 {chart_type.title()}",
                        thread_ts=thread_ts,
                    )
                except Exception as e:
                    logger.warning(f"Failed to upload chart: {e}")

        # Upload CSV if data available
        if result.has_data:
            csv_buffer = formatter.result_to_csv(result)
            if csv_buffer:
                try:
                    num_rows = len(result.result_set.get("data", []))
                    app.client.files_upload_v2(
                        channel=channel,
                        content=csv_buffer.getvalue(),
                        filename="data.csv",
                        title=f"📥 Data Export ({num_rows} rows)",
                        thread_ts=thread_ts,
                    )
                except Exception as e:
                    logger.warning(f"Failed to upload CSV: {e}")

        # Save context for multi-turn
        context.add_user_message(thread_ts, question)
        context.add_assistant_message(thread_ts, result.answer)

        # Tip on first message
        if len(history) == 0:
            say(
                "💡 _Tip: Reply in this thread for follow-up questions_",
                thread_ts=thread_ts,
            )

    except Exception as e:
        logger.exception("Error handling question")
        say(f"❌ Error: {str(e)}", thread_ts=thread_ts)


# ─── Testing ──────────────────────────────────────────────────────────────────


def test_agent(question: str = "Hello, are you working?"):
    """Test the agent without Slack."""
    print(f"\n🧪 Testing agent with question: '{question}'")
    print("-" * 60)

    client = get_agent_client()
    print(f"Agent: {client.agent_name}")
    print(f"Database: {client.database}.{client.schema}")
    print(f"Account: {client.account}")
    print()

    start = time.time()
    result = client.ask(question, progress_callback=lambda s: print(f"  ⏳ {s}"))
    elapsed = time.time() - start

    print()
    print(f"✅ Response ({elapsed:.1f}s):")
    print("-" * 60)
    print(result.answer[:1000])

    if result.has_sql:
        print()
        print("📊 SQL:")
        print(result.sql[:300])

    if result.has_data:
        rows = len(result.result_set.get("data", []))
        print(f"\n📋 Data: {rows} rows, columns: {result.column_names}")

    if result.chart_specs:
        print(f"\n📈 Charts: {len(result.chart_specs)}")

    print("-" * 60)
    print("Tools used:", result.tools_used)


# ─── Main ─────────────────────────────────────────────────────────────────────


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Snowflake Intelligence Slack Bot")
    parser.add_argument(
        "--test", "-t", type=str, help="Test with a question (no Slack)"
    )
    parser.add_argument(
        "--config", "-c", type=str, default="config.yml", help="Config file"
    )
    args = parser.parse_args()

    # Load config
    global CONFIG
    CONFIG = load_config(args.config)

    # Test mode
    if args.test:
        test_agent(args.test)
        return

    # Check environment
    missing = []
    for var in ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_PAT"]:
        if not os.getenv(var):
            missing.append(var)

    if missing:
        print(f"❌ Missing environment variables: {', '.join(missing)}")
        print("\nSet them and try again:")
        for var in missing:
            print(f"  export {var}='your-value'")
        return

    # Start Slack bot
    app = create_slack_app()

    if not app:
        print("⚠️  Slack not available. Running in test mode...")
        print("\nYou can still test the agent:")
        print("  python bot.py --test 'How many customers do we have?'")
        print("\nOr install Slack:")
        print("  pip install slack-bolt")
        return

    # Run bot
    print("🚀 Starting Slack Bot...")
    print(f"   Account: {os.getenv('SNOWFLAKE_ACCOUNT')}")
    print(
        f"   Default Agent: {CONFIG.get('agents', {}).get('default', {}).get('name')}"
    )
    print()

    try:
        SocketModeHandler(app, os.getenv("SLACK_APP_TOKEN")).start()
    except KeyboardInterrupt:
        print("\n👋 Shutting down...")


if __name__ == "__main__":
    main()
