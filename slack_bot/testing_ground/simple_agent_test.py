#!/usr/bin/env python3
"""
Simple Snowflake Agent API Test
Tests basic connectivity and captures real responses
"""

import sys
import json
from pathlib import Path
import logging

# Add the project paths
project_root = Path(__file__).parent.parent.parent
data_setup_path = project_root / "data_setup"
sys.path.insert(0, str(data_setup_path))

try:
    from snowflake_connection import SnowflakeConnection

    print("✅ Successfully imported snowflake_connection")
except ImportError as e:
    print(f"❌ Could not import snowflake_connection: {e}")
    sys.exit(1)

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_basic_connection():
    """Test basic Snowflake connection"""
    print("\n🔍 Testing Snowflake connection...")

    try:
        # Use the specific Snow CLI connection
        connection = SnowflakeConnection.from_snow_cli("snowflake_intelligence")

        print(
            f"✅ Connected to {connection.current_database}.{connection.current_schema}"
        )
        print(f"   Warehouse: {connection.current_warehouse}")

        return connection

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None


def test_agent_access(connection):
    """Test access to agents"""
    print("\n🤖 Testing agent access...")

    try:
        # Switch to agents schema
        connection.execute("USE DATABASE SNOWFLAKE_INTELLIGENCE")
        # Don't need to USE SCHEMA since we'll query at database level

        print(
            f"✅ Switched to {connection.current_database}.{connection.current_schema}"
        )

        # List agents
        agents_df = connection.sql("SHOW AGENTS IN DATABASE SNOWFLAKE_INTELLIGENCE")
        agents = agents_df.collect()

        print(f"✅ Found {len(agents)} agents:")
        for agent in agents:
            print(f"   - {agent['name']}")

        return [agent["name"] for agent in agents]

    except Exception as e:
        print(f"❌ Agent access failed: {e}")
        return []


def test_agent_describe(connection, agent_names):
    """Test describing agents"""
    print("\n📋 Testing agent descriptions...")

    for agent_name in agent_names:
        try:
            print(f"\n🔍 Describing {agent_name}:")
            desc_df = connection.sql(f"DESC AGENT {agent_name}")
            details = desc_df.collect()

            for detail in details[:5]:  # Show first 5 properties
                prop = detail.get("property", "unknown")
                value = str(detail.get("value", ""))[:100]  # Truncate long values
                print(f"   {prop}: {value}")

            if len(details) > 5:
                print(f"   ... and {len(details) - 5} more properties")

        except Exception as e:
            print(f"❌ Failed to describe {agent_name}: {e}")


def test_simple_queries(connection, agent_names):
    """Test simple queries to agents (if possible)"""
    print("\n💬 Testing simple agent queries...")

    # Simple test questions
    test_questions = [
        "Hello, are you working?",
        "What can you help me with?",
        "Show me a simple summary",
    ]

    for agent_name in agent_names[:1]:  # Test just the first agent
        print(f"\n🧪 Testing {agent_name}:")

        for question in test_questions:
            try:
                print(f"   Question: {question}")

                # Try a direct SQL approach first (this may not work for agents)
                # This is just to see what happens
                result = connection.sql(f"SELECT '{question}' as test_query").collect()
                print(f"   Basic SQL test: {result[0]['TEST_QUERY']}")

                break  # Just test one question for now

            except Exception as e:
                print(f"   ❌ Query failed: {e}")
                break


def main():
    """Main testing function"""
    print("🚀 Starting Snowflake Agent API Testing")
    print("=" * 50)

    # Test connection
    connection = test_basic_connection()
    if not connection:
        print("\n❌ Cannot proceed without connection")
        return

    # Test agent access
    agent_names = test_agent_access(connection)
    if not agent_names:
        print("\n❌ Cannot proceed without agents")
        return

    # Test agent descriptions
    test_agent_describe(connection, agent_names)

    # Test simple queries
    test_simple_queries(connection, agent_names)

    print("\n" + "=" * 50)
    print("✅ Basic testing complete!")
    print("\n💡 Next steps:")
    print("   1. Run api_response_analyzer.py to test actual API calls")
    print("   2. Check examples.md for documented findings")
    print("   3. Try the Snowflake Agent REST API directly")

    # Close connection
    connection.close()


if __name__ == "__main__":
    main()
