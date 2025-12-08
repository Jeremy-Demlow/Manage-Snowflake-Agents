#!/usr/bin/env python3
"""
Test Agent Object API with actual PAT token
This will show us the real response structure from Snowflake Intelligence layers
"""

import requests
import json
import time

# Configuration - set via environment variables
# export SNOWFLAKE_ACCOUNT="your-account"
# export SNOWFLAKE_PAT="your-pat-token"
import os

ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "your-account")
DATABASE = "SNOWFLAKE_INTELLIGENCE"
SCHEMA = "AGENTS"
PAT_TOKEN = os.environ.get("SNOWFLAKE_PAT", "")

if not PAT_TOKEN:
    print("⚠️  SNOWFLAKE_PAT not set. Set it with: export SNOWFLAKE_PAT='your-token'")
    exit(1)

BASE_URL = f"https://{ACCOUNT}.snowflakecomputing.com"


def test_agent_call(agent_name: str, question: str):
    """Test calling an agent with the Agent Object API"""
    print(f"\n{'='*70}")
    print(f"🤖 Testing Agent: {agent_name}")
    print(f"❓ Question: {question}")
    print(f"{'='*70}\n")

    # Construct Agent Object API endpoint
    endpoint = f"{BASE_URL}/api/v2/databases/{DATABASE}/schemas/{SCHEMA}/agents/{agent_name}:run"

    # Simple request payload
    payload = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": question}]}]
    }

    # Try different header combinations
    auth_variants = [
        {
            "name": "KEYPAIR_JWT",
            "headers": {
                "Authorization": f"Bearer {PAT_TOKEN}",
                "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        },
        {
            "name": "PROGRAMMATIC_ACCESS_TOKEN",
            "headers": {
                "Authorization": f"Bearer {PAT_TOKEN}",
                "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        },
        {
            "name": "Simple Bearer",
            "headers": {
                "Authorization": f"Bearer {PAT_TOKEN}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        },
    ]

    print(f"📡 Calling: {endpoint}\n")

    for variant in auth_variants:
        print(f"🔑 Trying auth variant: {variant['name']}")

        start_time = time.time()
        headers = variant["headers"]

        try:
            # Make the request with streaming
            response = requests.post(
                endpoint, json=payload, headers=headers, timeout=60, stream=True
            )

            elapsed = time.time() - start_time

            print(f"   ⏱️  Response time: {elapsed:.2f}s")
            print(f"   📊 Status code: {response.status_code}")

            if response.status_code == 200:
                print(f"   ✅ SUCCESS with {variant['name']}!\n")
                break  # Use this variant
            else:
                print(f"   ❌ Failed: {response.text[:150]}\n")
                continue

        except Exception as e:
            print(f"   ❌ Request failed: {e}\n")
            continue

    # If none worked, return error
    if response.status_code != 200:
        print(f"❌ All authentication variants failed")
        return {
            "agent": agent_name,
            "question": question,
            "error": "Authentication failed with all variants",
        }

    # Parse successful response
    print("✅ Success! Agent responded.\n")
    print("📋 Streaming Response Events:")
    print("-" * 70)

    events = []
    event_count = 0
    current_event = None
    elapsed = time.time() - start_time

    # Parse Server-Sent Events
    for line in response.iter_lines(decode_unicode=True):
        if line.startswith("event: "):
            current_event = line[7:].strip()
            event_count += 1
            print(f"\n🔔 Event #{event_count}: {current_event}")

        elif line.startswith("data: "):
            try:
                data = json.loads(line[6:])
                events.append({"event_type": current_event, "data": data})

                # Pretty print the data
                data_preview = json.dumps(data, indent=2)
                if len(data_preview) > 500:
                    data_preview = data_preview[:500] + "\n    ... (truncated)"
                print(f"   📦 Data: {data_preview}")

            except json.JSONDecodeError as e:
                print(f"   ⚠️  Could not parse JSON: {e}")
                print(f"   Raw: {line[6:][:200]}")

    # Save full response
    result = {
        "agent": agent_name,
        "question": question,
        "status_code": response.status_code,
        "elapsed_time": elapsed,
        "event_count": event_count,
        "events": events,
    }

    output_file = f"agent_response_{agent_name}_{int(time.time())}.json"
    with open(output_file, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\n💾 Full response saved to: {output_file}")

    # Analyze the response
    print("\n📈 Response Analysis:")
    print(f"   Total events: {event_count}")

    # Extract key information
    text_content = []
    tool_uses = []
    status_updates = []

    for event in events:
        event_type = event.get("event_type", "")
        data = event.get("data", {})

        if "text" in data:
            text_content.append(data["text"])
        if "type" in data and "cortex" in str(data.get("type", "")).lower():
            tool_uses.append(data.get("type"))
        if "status" in data:
            status_updates.append(data.get("message", data.get("status")))

    if text_content:
        full_text = "".join(text_content)
        print(f"   📝 Response text length: {len(full_text)} chars")
        print(f"   💬 Preview: {full_text[:200]}...")

    if tool_uses:
        print(f"   🔧 Tools used: {', '.join(set(tool_uses))}")

    if status_updates:
        print(f"   📊 Status updates: {len(status_updates)}")
        for status in status_updates[:3]:
            print(f"      - {status}")

    return result


def main():
    """Test different agents with different question types"""
    print("\n🚀 Testing Snowflake Agent Object API with PAT")
    print("Using existing Snowflake Intelligence layers\n")

    # Test cases - start with simple ones
    test_cases = [
        ("ACME_INTELLIGENCE_AGENT", "How many customers do we have?"),
        # Uncomment to test more:
        # ("ACME_CONTRACTS_AGENT", "Which contracts are at risk of churn?"),
        # ("DATA_ENGINEER_ASSISTANT", "What are the slowest queries today?"),
    ]

    results = []

    for agent_name, question in test_cases:
        result = test_agent_call(agent_name, question)
        results.append(result)
        time.sleep(2)  # Small delay between tests

    # Summary
    print("\n" + "=" * 70)
    print("📊 Test Summary")
    print("=" * 70)

    successful = [r for r in results if r.get("status_code") == 200]
    print(f"\n✅ Successful: {len(successful)}/{len(results)} tests")

    if successful:
        print("\n🎉 Agent Object API is working!")
        print("Now we can build the Slack bot using these patterns.")
    else:
        print("\n⚠️  Some tests failed. Check the errors above.")

    print("\n📝 Next steps:")
    print("   1. Review the saved JSON response files")
    print("   2. Update examples.md with real response structure")
    print("   3. Build Slack bot using proven Agent Object API pattern")


if __name__ == "__main__":
    main()
