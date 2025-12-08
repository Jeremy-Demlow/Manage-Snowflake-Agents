#!/usr/bin/env python3
"""
Quick bot health check - run before deploying
Good engineering practice: Test your code!
"""

import os
import sys

# Don't set Slack tokens - let them be None so Slack doesn't initialize
os.environ["SNOWFLAKE_PAT"] = "test"

print("🧪 Testing bot health...")
print()

# Test 1: Imports work
print("1. Testing imports...")
try:
    import simple_bot
    from simple_bot import format_for_slack, AGENTS, ACCOUNT

    print("   ✅ Imports successful")
    print(f"   Slack available: {simple_bot.SLACK_AVAILABLE}")
except Exception as e:
    print(f"   ❌ Import failed: {e}")
    sys.exit(1)

# Test 2: Configuration valid
print("2. Testing configuration...")
try:
    assert AGENTS, "AGENTS dict is empty"
    assert "intelligence" in AGENTS, "Missing intelligence agent"
    assert ACCOUNT, "Account not configured"
    print(f"   ✅ Config valid (Account: {ACCOUNT}, {len(AGENTS)} agents)")
except Exception as e:
    print(f"   ❌ Config failed: {e}")
    sys.exit(1)

# Test 3: Formatting function works
print("3. Testing response formatting...")
try:
    test_answer = "Test answer with **bold** and details"
    formatted = format_for_slack(test_answer, "test question")
    assert isinstance(formatted, str), "Formatter didn't return string"
    print(f"   ✅ Formatting works")
except Exception as e:
    print(f"   ❌ Formatting failed: {e}")
    sys.exit(1)

# Test 4: Check required environment variables
print("4. Checking environment variables...")
required_vars = {
    "SNOWFLAKE_PAT": "Snowflake Personal Access Token",
    "SLACK_BOT_TOKEN": "Slack Bot Token",
    "SLACK_APP_TOKEN": "Slack App Token",
}

missing = []
for var, desc in required_vars.items():
    val = os.getenv(var)
    if not val or val == "test" or val == "your-pat-here":
        missing.append(f"{var} ({desc})")
        print(f"   ⚠️  {var} not set")
    else:
        print(f"   ✅ {var} configured")

if missing:
    print()
    print("⚠️  Missing environment variables:")
    for m in missing:
        print(f"   - {m}")
    print()
    print("Set them before running:")
    print("export SNOWFLAKE_PAT='your-token'")
    print("export SLACK_BOT_TOKEN='xoxb-...'")
    print("export SLACK_APP_TOKEN='xapp-...'")

# Test 5: Thread context storage works
print("5. Testing thread context...")
try:
    import simple_bot

    test_thread = "test_123"
    simple_bot.thread_context[test_thread] = [
        {"role": "user", "content": [{"type": "text", "text": "test"}]}
    ]
    assert test_thread in simple_bot.thread_context
    print("   ✅ Thread context storage works")
except Exception as e:
    print(f"   ❌ Thread context failed: {e}")

# Test 6: Core modules imported correctly
print("6. Testing core dependencies...")
try:
    assert hasattr(simple_bot, "time"), "time module not imported"
    assert hasattr(simple_bot, "requests"), "requests module not imported"
    assert hasattr(simple_bot, "json"), "json module not imported"
    print("   ✅ All dependencies present")
except Exception as e:
    print(f"   ❌ Missing dependencies: {e}")

print()
print("=" * 50)
if not missing:
    print("✅ All checks passed! Bot is ready to run.")
    print()
    print("Start with: python simple_bot.py")
else:
    print("⚠️  Configure missing variables before running")
print("=" * 50)
