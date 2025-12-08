#!/usr/bin/env python3
"""
Setup script for Snowflake Agent deployment environment.
Verifies permissions and creates required objects using SnowflakeConnection.
"""

import sys
from pathlib import Path


from snowflake_connection import SnowflakeConnection, ConnectionConfig, ConfigurationError, ConnectionError
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def check_connection(connection_name: str = 'snowflake_agents') -> SnowflakeConnection:
    """Verify Snow CLI connection works and return connection."""
    print(f"\n🔍 Checking connection '{connection_name}'...")

    try:
        conn = SnowflakeConnection.from_snow_cli(connection_name)
        conn.test_connection()
        print(f"✅ Connection '{connection_name}' is working")
        print(f"   Database: {conn.current_database}")
        print(f"   Schema: {conn.current_schema}")
        print(f"   Warehouse: {conn.current_warehouse}")
        return conn
    except ConfigurationError as e:
        print(f"❌ Connection not configured: {e}")
        print(f"   Run: snow connection add {connection_name}")
        return None
    except ConnectionError as e:
        print(f"❌ Connection failed: {e}")
        return None


def setup_databases(conn: SnowflakeConnection) -> bool:
    """Create AGENTS schema in the data database."""
    print("\n📦 Setting up agent schema...")

    try:
        # Create AGENTS schema in the data database (agents live with data)
        conn.execute("CREATE SCHEMA IF NOT EXISTS SKI_RESORT_DB.AGENTS")
        print("   ✓ SKI_RESORT_DB.AGENTS")
        return True
    except Exception as e:
        print(f"   ⚠️ Warning: {e}")
        return False


def check_cortex_access(conn: SnowflakeConnection) -> bool:
    """Verify Cortex access is available."""
    print("\n🧠 Checking Cortex access...")

    try:
        result = conn.fetch(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', 'Say hello in 3 words') AS test"
        )
        if result:
            print(f"✅ Cortex access confirmed")
            print(f"   Response: {result[0]['TEST'][:50]}...")
        return True
    except Exception as e:
        print(f"⚠️ Cortex access may not be available: {str(e)[:100]}")
        return False


def check_semantic_views(conn: SnowflakeConnection) -> bool:
    """Verify semantic views exist."""
    print("\n📊 Checking semantic views in SKI_RESORT_DB.SEMANTIC...")

    try:
        result = conn.fetch("SHOW SEMANTIC VIEWS IN SCHEMA SKI_RESORT_DB.SEMANTIC")

        if result:
            views = [row['name'] for row in result]
            print(f"✅ Found {len(views)} semantic views:")
            for v in views:
                print(f"   - {v}")
            return True
        else:
            print("⚠️ No semantic views found - run dbt first!")
            return False
    except Exception as e:
        print(f"⚠️ Could not check semantic views: {e}")
        return False


def check_snowflake_intelligence(conn: SnowflakeConnection) -> bool:
    """Check if Snowflake Intelligence object exists."""
    print("\n🧊 Checking Snowflake Intelligence...")

    try:
        result = conn.fetch("SHOW SNOWFLAKE INTELLIGENCES")

        if result:
            print("   ✅ Snowflake Intelligence object exists")
            print("   Agents will be added for UI visibility after deployment")
        else:
            print("   ℹ️  No Snowflake Intelligence object")
            print("   Agents can still be used via SQL, just won't appear in SI UI")
        return True
    except Exception as e:
        print(f"   ℹ️  Could not check Snowflake Intelligence: {e}")
        return False


def check_agents(conn: SnowflakeConnection) -> bool:
    """Check existing agents in SKI_RESORT_DB.AGENTS."""
    print(f"\n🤖 Checking existing agents in SKI_RESORT_DB.AGENTS...")

    try:
        result = conn.fetch("SHOW AGENTS IN SCHEMA SKI_RESORT_DB.AGENTS")

        if result:
            print(f"   Found {len(result)} agents:")
            for row in result:
                print(f"   - {row['name']}")
        else:
            print("   No agents deployed yet")
        return True
    except Exception as e:
        print(f"   Could not list agents (schema may not exist yet): {e}")
        return False


def main():
    print("=" * 60)
    print("SNOWFLAKE AGENT SETUP & VERIFICATION")
    print("=" * 60)

    connection_name = 'snowflake_agents'

    # Check connection
    conn = check_connection(connection_name)
    if not conn:
        print("\n❌ Setup failed: Cannot connect to Snowflake")
        sys.exit(1)

    try:
        # Setup databases
        setup_databases(conn)

        # Check Cortex
        check_cortex_access(conn)

        # Check semantic views
        check_semantic_views(conn)

        # Check existing agents
        check_agents(conn)

        # Check Snowflake Intelligence
        check_snowflake_intelligence(conn)

    finally:
        conn.close()

    print("\n" + "=" * 60)
    print("SETUP COMPLETE")
    print("=" * 60)
    print("""
Next steps:

1. If semantic views are missing, run dbt:
   cd dbt_ski_resort && dbt run --profiles-dir .

2. Deploy agents:
   python deploy.py --all --env dev

3. View agents in Snowsight:
   AI & ML → Agents
""")


if __name__ == "__main__":
    main()
