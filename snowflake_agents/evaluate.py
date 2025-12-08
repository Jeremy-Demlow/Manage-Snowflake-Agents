#!/usr/bin/env python3
"""
Agent Verification and Test Planning
=====================================

This script verifies agent configurations and generates test plans.
For actual agent evaluation (calling agents with questions), use:

1. **Snowsight UI**: AI & ML → Agents → [Agent] → Test
2. **TruLens**: python trulens_eval.py --agent <name> --env dev

This script provides:
- Agent existence verification
- Configuration validation
- Golden question test plan generation
- Evaluation results viewing

Usage:
    python evaluate.py --verify --env dev
    python evaluate.py --agent ski_ops_assistant --env dev
    python evaluate.py --results --env dev
"""

import os
import sys
import yaml
import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional

from snowflake_connection import SnowflakeConnection

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class AgentVerifier:
    """Verify agent deployments and configuration."""

    AGENTS_DIR = Path(__file__).parent / 'agents'
    TESTS_DIR = AGENTS_DIR / 'tests'

    def __init__(self, agent_name: str, environment: str):
        self.agent_name = agent_name
        self.environment = environment

        # Load configs
        self.env_config = self._load_env_config()
        self.golden_questions = self._load_golden_questions()

        # Determine agent fully qualified name
        suffix = self.env_config.get('settings', {}).get('version_suffix', '')
        self.agent_fqn = self._get_agent_fqn(suffix)

    def _load_env_config(self) -> Dict:
        """Load environment configuration."""
        env_file = self.AGENTS_DIR / 'environments' / f"{self.environment}.yml"
        with open(env_file) as f:
            return yaml.safe_load(f)

    def _load_golden_questions(self) -> Dict:
        """Load golden questions for all agents."""
        questions_file = self.TESTS_DIR / 'golden_questions.yml'
        with open(questions_file) as f:
            return yaml.safe_load(f)

    def _get_agent_fqn(self, suffix: str) -> str:
        """Get fully qualified agent name."""
        deployment = self.env_config.get('agent_deployment', {})
        db = deployment.get('database', 'SKI_RESORT_DB')
        schema = deployment.get('schema', 'AGENTS')

        name_map = {
            'ski_ops_assistant': 'SKI_OPS_ASSISTANT',
            'customer_insights': 'CUSTOMER_INSIGHTS',
            'resort_executive': 'RESORT_EXECUTIVE'
        }
        agent_upper = name_map.get(self.agent_name, self.agent_name.upper())

        if suffix:
            agent_upper = f"{agent_upper}{suffix.upper()}"

        return f"{db}.{schema}.{agent_upper}"

    def get_questions(self) -> Dict[str, List]:
        """Get questions for this agent from golden questions file."""
        agent_questions = self.golden_questions.get(self.agent_name, {})
        return {
            'in_scope': agent_questions.get('in_scope', []),
            'out_of_scope': agent_questions.get('out_of_scope', [])
        }

    def verify_agent_exists(self, conn: SnowflakeConnection) -> bool:
        """Verify the agent exists in Snowflake."""

        deployment = self.env_config.get('agent_deployment', {})
        db = deployment.get('database', 'SKI_RESORT_DB')
        schema = deployment.get('schema', 'AGENTS')

        try:
            result = conn.fetch(f"SHOW AGENTS IN SCHEMA {db}.{schema}")

            # Check if our agent is in the list
            suffix = self.env_config.get('settings', {}).get('version_suffix', '')
            name_map = {
                'ski_ops_assistant': 'SKI_OPS_ASSISTANT',
                'customer_insights': 'CUSTOMER_INSIGHTS',
                'resort_executive': 'RESORT_EXECUTIVE'
            }
            expected_name = name_map.get(self.agent_name, self.agent_name.upper())
            if suffix:
                expected_name = f"{expected_name}{suffix.upper()}"

            for row in result:
                # Row objects use bracket notation
                row_dict = row.as_dict() if hasattr(row, 'as_dict') else dict(row)
                if row_dict.get('name') == expected_name:
                    return True

            return False

        except Exception as e:
            logger.error(f"Error checking agent: {e}")
            return False

    def get_agent_details(self, conn: SnowflakeConnection) -> Dict:
        """Get agent details from Snowflake."""

        deployment = self.env_config.get('agent_deployment', {})
        db = deployment.get('database', 'SKI_RESORT_DB')
        schema = deployment.get('schema', 'AGENTS')

        try:
            result = conn.fetch(f"SHOW AGENTS IN SCHEMA {db}.{schema}")

            suffix = self.env_config.get('settings', {}).get('version_suffix', '')
            name_map = {
                'ski_ops_assistant': 'SKI_OPS_ASSISTANT',
                'customer_insights': 'CUSTOMER_INSIGHTS',
                'resort_executive': 'RESORT_EXECUTIVE'
            }
            expected_name = name_map.get(self.agent_name, self.agent_name.upper())
            if suffix:
                expected_name = f"{expected_name}{suffix.upper()}"

            for row in result:
                # Row objects use bracket notation
                row_dict = row.as_dict() if hasattr(row, 'as_dict') else dict(row)
                if row_dict.get('name') == expected_name:
                    return {
                        'name': row_dict.get('name'),
                        'database': row_dict.get('database_name'),
                        'schema': row_dict.get('schema_name'),
                        'created': row_dict.get('created_on'),
                        'comment': (row_dict.get('comment') or '')[:100],
                        'profile': row_dict.get('profile', '')
                    }

            return {}

        except Exception as e:
            logger.error(f"Error getting agent details: {e}")
            return {}

    def generate_test_plan(self) -> Dict:
        """Generate a test plan from golden questions."""

        questions = self.get_questions()

        test_plan = {
            'agent': self.agent_name,
            'agent_fqn': self.agent_fqn,
            'environment': self.environment,
            'generated': datetime.now().isoformat(),
            'in_scope_tests': [],
            'out_of_scope_tests': []
        }

        for q in questions['in_scope']:
            test_plan['in_scope_tests'].append({
                'question': q.get('question'),
                'expected_tool': q.get('expected_tool'),
                'validation': 'Expect substantive answer using the specified tool'
            })

        for q in questions['out_of_scope']:
            test_plan['out_of_scope_tests'].append({
                'question': q.get('question'),
                'expected_decline': q.get('expected_decline'),
                'validation': 'Expect polite decline or redirect'
            })

        return test_plan

    def print_test_plan(self, test_plan: Dict):
        """Print test plan in a readable format."""

        print("\n" + "=" * 70)
        print(f"TEST PLAN: {test_plan['agent']}")
        print("=" * 70)
        print(f"Agent: {test_plan['agent_fqn']}")
        print(f"Environment: {test_plan['environment']}")
        print()

        print("IN-SCOPE QUESTIONS (expect answers):")
        print("-" * 40)
        for i, test in enumerate(test_plan['in_scope_tests'], 1):
            print(f"  {i}. {test['question']}")
            print(f"     → Expected tool: {test['expected_tool']}")
        print()

        print("OUT-OF-SCOPE QUESTIONS (expect decline):")
        print("-" * 40)
        for i, test in enumerate(test_plan['out_of_scope_tests'], 1):
            print(f"  {i}. {test['question']}")
            print(f"     → Expected: {test['expected_decline']}")
        print()

        print("HOW TO TEST:")
        print("-" * 40)
        print("  Option 1: Snowsight UI")
        print("    1. Go to AI & ML → Agents")
        print(f"    2. Select: {test_plan['agent_fqn'].split('.')[-1]}")
        print("    3. Click 'Test' and enter questions above")
        print()
        print("  Option 2: TruLens Evaluation")
        print(f"    python trulens_eval.py --agent {test_plan['agent']} --env {test_plan['environment']}")
        print()

    def run_verification(self) -> Dict:
        """Run verification for this agent."""

        logger.info("=" * 70)
        logger.info(f"VERIFYING AGENT: {self.agent_name}")
        logger.info(f"Environment: {self.environment}")
        logger.info(f"Agent FQN: {self.agent_fqn}")
        logger.info("=" * 70)

        # Connect to Snowflake
        connection_name = self.env_config.get('snowflake', {}).get('connection_name', 'snowflake_agents')
        conn = SnowflakeConnection.from_snow_cli(connection_name)

        results = {
            'agent': self.agent_name,
            'agent_fqn': self.agent_fqn,
            'environment': self.environment,
            'timestamp': datetime.now().isoformat(),
            'exists': False,
            'details': {},
            'test_plan': {}
        }

        try:
            # Check if agent exists
            logger.info("Checking agent exists...")
            results['exists'] = self.verify_agent_exists(conn)

            if results['exists']:
                logger.info(f"  ✅ Agent exists")

                # Get details
                results['details'] = self.get_agent_details(conn)
                if results['details']:
                    logger.info(f"  Created: {results['details'].get('created')}")
                    logger.info(f"  Comment: {results['details'].get('comment', 'N/A')[:50]}...")
            else:
                logger.warning(f"  ❌ Agent not found")
                logger.info(f"  Deploy with: python deploy.py --agent {self.agent_name} --env {self.environment}")

            # Generate test plan
            results['test_plan'] = self.generate_test_plan()

        finally:
            conn.close()

        return results


def view_evaluation_results(environment: str = 'dev'):
    """View past evaluation results from the database."""

    # Load env config
    env_file = Path(__file__).parent / 'agents' / 'environments' / f"{environment}.yml"
    with open(env_file) as f:
        env_config = yaml.safe_load(f)

    deployment = env_config.get('agent_deployment', {})
    db = deployment.get('database', 'SKI_RESORT_DB')
    schema = deployment.get('schema', 'AGENTS')

    connection_name = env_config.get('snowflake', {}).get('connection_name', 'snowflake_agents')
    conn = SnowflakeConnection.from_snow_cli(connection_name)

    try:
        logger.info(f"Checking for evaluation results in {db}.{schema}...")

        # Check if table exists
        result = conn.fetch(f"""
        SELECT TABLE_NAME
        FROM {db}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}'
        AND TABLE_NAME = 'AGENT_EVALUATIONS'
        """)

        if not result:
            logger.info("  No evaluation results found (AGENT_EVALUATIONS table doesn't exist)")
            logger.info("  Run TruLens evaluation to generate results")
            return

        # Get recent evaluations
        results = conn.fetch(f"""
        SELECT
            AGENT_NAME,
            ENVIRONMENT,
            EVALUATED_AT,
            IN_SCOPE_PASSED,
            IN_SCOPE_TOTAL,
            OUT_OF_SCOPE_PASSED,
            OUT_OF_SCOPE_TOTAL,
            OVERALL_RATE
        FROM {db}.{schema}.AGENT_EVALUATIONS
        ORDER BY EVALUATED_AT DESC
        LIMIT 10
        """)

        if results:
            print("\nRecent Evaluation Results:")
            print("-" * 80)
            print(f"{'Agent':<25} {'Env':<10} {'In-Scope':<15} {'Out-Scope':<15} {'Rate':<10}")
            print("-" * 80)
            for r in results:
                in_scope = f"{r['IN_SCOPE_PASSED']}/{r['IN_SCOPE_TOTAL']}"
                out_scope = f"{r['OUT_OF_SCOPE_PASSED']}/{r['OUT_OF_SCOPE_TOTAL']}"
                rate = f"{r['OVERALL_RATE']*100:.0f}%" if r['OVERALL_RATE'] else "N/A"
                print(f"{r['AGENT_NAME']:<25} {r['ENVIRONMENT']:<10} {in_scope:<15} {out_scope:<15} {rate:<10}")
        else:
            logger.info("  No evaluation results found")

    finally:
        conn.close()


def list_agents() -> List[str]:
    """List agents that have golden questions."""
    questions_file = Path(__file__).parent / 'agents' / 'tests' / 'golden_questions.yml'
    with open(questions_file) as f:
        questions = yaml.safe_load(f)

    return [k for k in questions.keys() if k not in ['version', 'created', 'evaluation_strategy']]


def verify_all_agents(environment: str = 'dev'):
    """Verify all agents exist."""

    agents = list_agents()
    results = []

    for agent in agents:
        verifier = AgentVerifier(agent, environment)
        result = verifier.run_verification()
        results.append(result)

    # Summary
    print("\n" + "=" * 70)
    print("VERIFICATION SUMMARY")
    print("=" * 70)

    all_exist = True
    for r in results:
        status = "✅" if r['exists'] else "❌"
        print(f"  {status} {r['agent']}: {r['agent_fqn']}")
        if not r['exists']:
            all_exist = False

    print()
    if all_exist:
        print("All agents verified. Test them in Snowsight or with TruLens.")
    else:
        print("Some agents missing. Deploy with: python deploy.py --all --env dev")


def main():
    parser = argparse.ArgumentParser(
        description='Verify Snowflake Intelligence Agents',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script verifies agents and generates test plans.

For actual agent testing, use:
  1. Snowsight UI: AI & ML → Agents → [Agent] → Test
  2. TruLens: python trulens_eval.py --agent <name> --env dev

Examples:
  python evaluate.py --verify --env dev        # Verify all agents
  python evaluate.py --agent ski_ops_assistant # Show test plan
  python evaluate.py --results --env dev       # View past results
  python evaluate.py --list                    # List agents
        """
    )

    parser.add_argument('--agent', '-a', help='Agent name to verify')
    parser.add_argument('--env', '-e', default='dev',
                       choices=['dev', 'staging', 'prod'],
                       help='Target environment (default: dev)')
    parser.add_argument('--verify', '-v', action='store_true',
                       help='Verify all agents exist')
    parser.add_argument('--results', '-r', action='store_true',
                       help='View past evaluation results')
    parser.add_argument('--list', '-l', action='store_true',
                       help='List agents with golden questions')

    args = parser.parse_args()

    if args.list:
        print("\nAgents with golden questions:")
        for agent in list_agents():
            print(f"  - {agent}")
        return

    if args.results:
        view_evaluation_results(args.env)
        return

    if args.verify:
        verify_all_agents(args.env)
        return

    if args.agent:
        verifier = AgentVerifier(args.agent, args.env)
        result = verifier.run_verification()

        if result['exists']:
            verifier.print_test_plan(result['test_plan'])
        return

    # Default: verify all
    verify_all_agents(args.env)


if __name__ == "__main__":
    main()
