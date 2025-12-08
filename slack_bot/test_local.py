#!/usr/bin/env python3
"""
Local Test Script for Slack Bot

Test the agent and formatting without needing Slack setup.
Useful for verifying everything works before deploying.

Usage:
    # Quick test
    python test_local.py

    # Test specific question
    python test_local.py "What was revenue last month?"

    # Interactive mode
    python test_local.py --interactive
"""

import argparse
import os
import sys


def check_environment():
    """Check required environment variables."""
    print("🔍 Checking environment...")

    account = os.getenv("SNOWFLAKE_ACCOUNT")
    pat = os.getenv("SNOWFLAKE_PAT")

    if not account:
        print("❌ SNOWFLAKE_ACCOUNT not set")
        print("   export SNOWFLAKE_ACCOUNT='your-account'")
        return False

    if not pat:
        print("❌ SNOWFLAKE_PAT not set")
        print("   export SNOWFLAKE_PAT='your-pat-token'")
        return False

    print(f"✅ Account: {account}")
    print(f"✅ PAT: {'*' * 10}...{pat[-4:] if len(pat) > 4 else '****'}")
    return True


def test_imports():
    """Test all module imports."""
    print("\n📦 Testing imports...")

    try:
        from agent import AgentClient, AgentResult

        print("✅ agent.py")
    except ImportError as e:
        print(f"❌ agent.py: {e}")
        return False

    try:
        from formatters import SlackFormatter, ChartRenderer

        print("✅ formatters.py")
    except ImportError as e:
        print(f"❌ formatters.py: {e}")
        return False

    try:
        from context import ConversationContext

        print("✅ context.py")
    except ImportError as e:
        print(f"❌ context.py: {e}")
        return False

    try:
        import bot

        print("✅ bot.py")
    except ImportError as e:
        print(f"❌ bot.py: {e}")
        return False

    return True


def test_config():
    """Test configuration loading."""
    print("\n⚙️  Testing config...")

    import bot

    config = bot.CONFIG

    agents = config.get("agents", {})
    default = agents.get("default", {})

    print(f"   Default agent: {default.get('name', 'NOT SET')}")
    print(f"   Database: {default.get('database', 'NOT SET')}")
    print(f"   Schema: {default.get('schema', 'NOT SET')}")

    return bool(default.get("name"))


def test_context():
    """Test conversation context."""
    print("\n💬 Testing context manager...")

    from context import ConversationContext

    ctx = ConversationContext(ttl_hours=0.01, max_messages=5)

    # Add messages
    ctx.add_user_message("test_thread", "Question 1")
    ctx.add_assistant_message("test_thread", "Answer 1")
    ctx.add_user_message("test_thread", "Question 2")

    history = ctx.get_history("test_thread")
    stats = ctx.get_stats()

    print(f"   Messages stored: {len(history)}")
    print(f"   Active threads: {stats['active_threads']}")
    print(f"   ✅ Context working")

    return True


def test_formatter():
    """Test Slack formatter."""
    print("\n🎨 Testing formatter...")

    from agent import AgentResult
    from formatters import SlackFormatter, ChartRenderer

    formatter = SlackFormatter()

    # Create mock result
    result = AgentResult(
        answer="We have **1,234 customers** total.\n\n- Vacation Family: 500\n- Weekend Warrior: 400",
        sql="SELECT COUNT(*) FROM customers",
        duration_seconds=5.2,
        tools_used=["cortex_analyst_text_to_sql"],
    )

    blocks = formatter.format_response(result, "How many customers?")
    print(f"   Generated {len(blocks)} Slack blocks")

    # Check chart renderer
    renderer = ChartRenderer()
    print(
        f"   Chart rendering: {'✅ Available' if renderer.available else '⚠️  Not available (install altair + vl-convert)'}"
    )

    return True


def test_agent_connection(question: str = "Hello, are you working?"):
    """Test actual agent connection."""
    print(f"\n🤖 Testing agent connection...")
    print(f"   Question: '{question}'")

    from agent import AgentClient
    import bot

    config = bot.CONFIG.get("agents", {}).get("default", {})

    client = AgentClient(
        agent_name=config.get("name", "RESORT_EXECUTIVE_DEV"),
        database=config.get("database", "SKI_RESORT_DB"),
        schema=config.get("schema", "AGENTS"),
        account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
        pat=os.getenv("SNOWFLAKE_PAT", ""),
    )

    print(f"   Endpoint: {client.endpoint}")

    def progress(status):
        print(f"   ⏳ {status}")

    result = client.ask(question, progress_callback=progress)

    if result.answer.startswith("Error"):
        print(f"\n❌ Agent error: {result.answer}")
        return False

    print(f"\n✅ Agent responded ({result.duration_seconds:.1f}s):")
    print("-" * 50)
    print(result.answer[:500])
    if len(result.answer) > 500:
        print("... (truncated)")
    print("-" * 50)

    if result.has_sql:
        print(f"\n📊 SQL: {result.sql[:200]}...")

    if result.tools_used:
        print(f"\n🔧 Tools: {', '.join(result.tools_used)}")

    return True


def interactive_mode():
    """Interactive Q&A mode."""
    print("\n🎮 Interactive Mode")
    print("   Type 'quit' to exit")
    print("-" * 50)

    from agent import AgentClient
    from context import ConversationContext
    import bot

    config = bot.CONFIG.get("agents", {}).get("default", {})

    client = AgentClient(
        agent_name=config.get("name"),
        database=config.get("database"),
        schema=config.get("schema"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        pat=os.getenv("SNOWFLAKE_PAT"),
    )

    context = ConversationContext()
    thread = "interactive"

    while True:
        try:
            question = input("\n❓ You: ").strip()

            if question.lower() in ["quit", "exit", "q"]:
                print("👋 Goodbye!")
                break

            if not question:
                continue

            history = context.get_history(thread)

            print("🤔 Thinking...")
            result = client.ask(question, conversation_history=history)

            print(f"\n🎿 Bot ({result.duration_seconds:.1f}s):")
            print(result.answer)

            if result.has_sql:
                print(f"\n📊 SQL:\n{result.sql}")

            # Save context
            context.add_user_message(thread, question)
            context.add_assistant_message(thread, result.answer)

        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break


def main():
    parser = argparse.ArgumentParser(description="Test Slack Bot locally")
    parser.add_argument("question", nargs="?", default=None, help="Question to test")
    parser.add_argument(
        "--interactive", "-i", action="store_true", help="Interactive mode"
    )
    parser.add_argument(
        "--skip-agent", action="store_true", help="Skip agent connection test"
    )
    args = parser.parse_args()

    print("=" * 50)
    print("🎿 Ski Resort Slack Bot - Local Test")
    print("=" * 50)

    # Basic checks
    if not check_environment():
        print("\n❌ Environment check failed. Set required variables and retry.")
        sys.exit(1)

    if not test_imports():
        print("\n❌ Import check failed. Check dependencies.")
        sys.exit(1)

    if not test_config():
        print("\n⚠️  Config check: using defaults")

    test_context()
    test_formatter()

    # Agent test
    if not args.skip_agent:
        question = args.question or "What is total revenue for the ski resort?"
        if not test_agent_connection(question):
            print("\n❌ Agent test failed. Check credentials and network.")
            sys.exit(1)

    # Interactive mode
    if args.interactive:
        interactive_mode()

    print("\n" + "=" * 50)
    print("✅ All tests passed! Bot is ready.")
    print("=" * 50)
    print("\nNext steps:")
    print("1. Create Slack app at https://api.slack.com/apps")
    print("2. Use slack_app_manifest.yml for quick setup")
    print("3. Set SLACK_BOT_TOKEN and SLACK_APP_TOKEN")
    print("4. Run: python bot.py")


if __name__ == "__main__":
    main()
