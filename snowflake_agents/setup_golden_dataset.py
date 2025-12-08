#!/usr/bin/env python3
"""
Setup Golden Q&A Dataset in Snowflake
======================================

Creates and populates the GOLDEN_QA_DATASET table in Snowflake for
TruLens evaluation with the `correctness` metric.

Per Snowflake AI Observability docs:
- source_type="TABLE" allows persistent evaluation datasets
- RECORD_ROOT.GROUND_TRUTH_OUTPUT enables correctness evaluation
- Dataset can be reused across multiple evaluation runs

Usage:
    python setup_golden_dataset.py --env dev
    python setup_golden_dataset.py --env dev --agent ski_ops_assistant
"""

import yaml
import argparse
from pathlib import Path
from datetime import datetime

from snowflake_connection import SnowflakeConnection

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


AGENTS_DIR = Path(__file__).parent / 'agents'


def load_golden_qa_dataset() -> dict:
    """Load golden Q&A dataset from YAML."""
    qa_file = AGENTS_DIR / 'tests' / 'golden_qa_dataset.yml'
    with open(qa_file) as f:
        return yaml.safe_load(f)


def load_env_config(environment: str) -> dict:
    """Load environment configuration."""
    env_file = AGENTS_DIR / 'environments' / f"{environment}.yml"
    with open(env_file) as f:
        return yaml.safe_load(f)


def setup_golden_dataset_table(conn: SnowflakeConnection, db: str, schema: str):
    """Create the GOLDEN_QA_DATASET table."""

    table_name = f"{db}.{schema}.GOLDEN_QA_DATASET"

    logger.info(f"Creating table: {table_name}")

    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        ID NUMBER AUTOINCREMENT,
        AGENT_NAME VARCHAR NOT NULL,
        USER_QUERY VARCHAR NOT NULL,
        GOLDEN_ANSWER VARCHAR NOT NULL,
        EXPECTED_TOOL VARCHAR,
        QUESTION_TYPE VARCHAR DEFAULT 'golden_qa',
        VERSION VARCHAR,
        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """)

    logger.info(f"  ✓ Table created: {table_name}")
    return table_name


def populate_golden_dataset(conn: SnowflakeConnection, db: str, schema: str,
                           agent_name: str = None):
    """Populate golden Q&A dataset from YAML."""

    table_name = f"{db}.{schema}.GOLDEN_QA_DATASET"
    golden_qa = load_golden_qa_dataset()
    version = golden_qa.get('version', '1.0')

    # Determine which agents to load (exclude metadata keys)
    agents_to_load = [agent_name] if agent_name else [
        k for k in golden_qa.keys() if k not in ['version', 'created', 'validated']
    ]

    total_loaded = 0

    for agent in agents_to_load:
        agent_data = golden_qa.get(agent, {})

        # Skip if not a dict (metadata fields)
        if not isinstance(agent_data, dict):
            continue

        questions = agent_data.get('questions', [])

        if not questions:
            logger.warning(f"  No questions found for {agent}")
            continue

        logger.info(f"Loading {len(questions)} Q&A pairs for {agent}...")

        # Clear existing data for this agent/version
        conn.execute(f"""
        DELETE FROM {table_name}
        WHERE AGENT_NAME = '{agent}' AND VERSION = '{version}'
        """)

        # Insert new data
        for qa in questions:
            question = qa.get('question', '').replace("'", "''")
            answer = qa.get('golden_answer', '').replace("'", "''")
            tool = qa.get('expected_tool', '')

            conn.execute(f"""
            INSERT INTO {table_name}
            (AGENT_NAME, USER_QUERY, GOLDEN_ANSWER, EXPECTED_TOOL, VERSION)
            VALUES (
                '{agent}',
                '{question}',
                '{answer}',
                '{tool}',
                '{version}'
            )
            """)
            total_loaded += 1

        logger.info(f"  ✓ Loaded {len(questions)} Q&A pairs for {agent}")

    logger.info(f"\n✓ Total loaded: {total_loaded} Q&A pairs")
    return total_loaded


def view_dataset(conn: SnowflakeConnection, db: str, schema: str):
    """View the golden dataset summary."""

    table_name = f"{db}.{schema}.GOLDEN_QA_DATASET"

    result = conn.fetch(f"""
    SELECT
        AGENT_NAME,
        VERSION,
        COUNT(*) as QA_PAIRS,
        MAX(UPDATED_AT) as LAST_UPDATED
    FROM {table_name}
    GROUP BY AGENT_NAME, VERSION
    ORDER BY AGENT_NAME
    """)

    if result:
        print("\n" + "=" * 60)
        print("GOLDEN Q&A DATASET SUMMARY")
        print("=" * 60)
        print(f"{'Agent':<25} {'Version':<10} {'Q&A Pairs':<10} {'Updated'}")
        print("-" * 60)
        for row in result:
            row_dict = row.as_dict() if hasattr(row, 'as_dict') else dict(row)
            print(f"{row_dict['AGENT_NAME']:<25} {row_dict['VERSION']:<10} {row_dict['QA_PAIRS']:<10} {row_dict['LAST_UPDATED']}")
    else:
        print("  No data in golden dataset table")


def main():
    parser = argparse.ArgumentParser(
        description='Setup Golden Q&A Dataset in Snowflake',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This creates the GOLDEN_QA_DATASET table used by TruLens for the
'correctness' metric. The correctness metric compares agent responses
against these verified golden answers.

Per Snowflake docs, the dataset_spec maps:
  - USER_QUERY → RECORD_ROOT.INPUT
  - GOLDEN_ANSWER → RECORD_ROOT.GROUND_TRUTH_OUTPUT

Examples:
  python setup_golden_dataset.py --env dev
  python setup_golden_dataset.py --env dev --agent ski_ops_assistant
  python setup_golden_dataset.py --env dev --view
        """
    )

    parser.add_argument('--env', '-e', default='dev',
                       choices=['dev', 'staging', 'prod'],
                       help='Target environment (default: dev)')
    parser.add_argument('--agent', '-a',
                       help='Specific agent to load (default: all)')
    parser.add_argument('--view', '-v', action='store_true',
                       help='View current dataset summary')

    args = parser.parse_args()

    # Load environment config
    env_config = load_env_config(args.env)
    deployment = env_config.get('agent_deployment', {})
    db = deployment.get('database', 'SKI_RESORT_DB')
    schema = deployment.get('schema', 'AGENTS')

    # Connect to Snowflake
    connection_name = env_config.get('snowflake', {}).get('connection_name', 'snowflake_agents')
    conn = SnowflakeConnection.from_snow_cli(connection_name)

    try:
        if args.view:
            view_dataset(conn, db, schema)
            return

        # Setup table
        setup_golden_dataset_table(conn, db, schema)

        # Populate data
        populate_golden_dataset(conn, db, schema, args.agent)

        # Show summary
        view_dataset(conn, db, schema)

        print("\n" + "=" * 60)
        print("NEXT STEPS")
        print("=" * 60)
        print("Run TruLens evaluation with correctness metric:")
        print(f"  python trulens_eval.py --agent <name> --env {args.env}")
        print()
        print("The correctness metric will compare agent responses against")
        print("the golden answers in this dataset.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
