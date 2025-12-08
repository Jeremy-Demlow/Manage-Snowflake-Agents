#!/usr/bin/env python3
"""
Snowflake Agent Deployment System
==================================
Deploy, version, and manage Cortex Agents for Snowflake Intelligence.

Usage:
    python deploy.py --agent ski_ops_assistant --env dev
    python deploy.py --agent customer_insights --env staging
    python deploy.py --agent resort_executive --env prod
    python deploy.py --all --env dev
    python deploy.py --agent ski_ops_assistant --env dev --dry-run
"""

import yaml
import json
import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

from snowflake_connection import SnowflakeConnection, ConnectionConfig

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class AgentDeployer:
    """Handles agent deployment with versioning and environment support."""

    AGENTS_DIR = Path(__file__).parent / 'agents'
    GENERATED_DIR = Path(__file__).parent / 'generated'

    def __init__(self, agent_name: str, environment: str, dry_run: bool = False):
        self.agent_name = agent_name
        self.environment = environment
        self.dry_run = dry_run

        # Load configs
        self.agent_config = self._load_agent_config()
        self.env_config = self._load_env_config()

        # Ensure output directory exists
        self.GENERATED_DIR.mkdir(exist_ok=True)

    def _load_agent_config(self) -> Dict:
        """Load agent configuration YAML."""
        agent_file = self.AGENTS_DIR / f"{self.agent_name}.yml"
        if not agent_file.exists():
            raise FileNotFoundError(f"Agent config not found: {agent_file}")

        with open(agent_file) as f:
            return yaml.safe_load(f)

    def _load_env_config(self) -> Dict:
        """Load environment configuration YAML."""
        env_file = self.AGENTS_DIR / 'environments' / f"{self.environment}.yml"
        if not env_file.exists():
            raise FileNotFoundError(f"Environment config not found: {env_file}")

        with open(env_file) as f:
            return yaml.safe_load(f)

    def build_specification(self) -> Dict:
        """Build the agent specification JSON for CREATE AGENT."""

        # Build tools array
        tools = []
        tool_resources = {}

        warehouse = self.env_config.get('warehouse', 'COMPUTE_WH')
        semantic_schema = self.env_config.get('data_sources', {}).get('semantic_schema', 'SEMANTIC')
        data_db = self.env_config.get('data_sources', {}).get('database', 'SKI_RESORT_DB')

        for tool in self.agent_config.get('tools', []):
            tool_name = tool['name']
            tool_type = tool['type']

            # Build tool spec
            tool_spec = {
                'tool_spec': {
                    'type': tool_type,
                    'name': tool_name,
                    'description': tool['description'].strip()
                }
            }

            # Add input schema for generic tools
            if tool_type == 'generic' and 'input_schema' in tool:
                tool_spec['tool_spec']['input_schema'] = tool['input_schema']

            tools.append(tool_spec)

            # Build tool resources
            if tool_type == 'cortex_analyst_text_to_sql':
                semantic_view = tool.get('semantic_view', {})
                db = semantic_view.get('database', data_db)
                schema = semantic_view.get('schema', semantic_schema)
                view_name = semantic_view.get('name', '')

                tool_resources[tool_name] = {
                    'semantic_view': f"{db}.{schema}.{view_name}",
                    'execution_environment': {
                        'type': 'warehouse',
                        'warehouse': warehouse
                    }
                }

            elif tool_type == 'generic' and 'procedure' in tool:
                proc = tool['procedure']
                tool_resources[tool_name] = {
                    'type': 'procedure',
                    'identifier': f"{proc['database']}.{proc['schema']}.{proc['name']}",
                    'name': proc.get('signature', proc['name']),
                    'execution_environment': {
                        'type': 'warehouse',
                        'warehouse': warehouse,
                        'query_timeout': proc.get('timeout', 120)
                    }
                }

            elif tool_type == 'procedure':
                # Direct procedure type (stored procedure as tool)
                # Tool spec type must be 'generic' for custom tools
                tool_spec['tool_spec']['type'] = 'generic'

                identifier = tool.get('identifier', {})
                proc_db = identifier.get('database', data_db)
                proc_schema = identifier.get('schema', 'MARTS')
                proc_name = identifier.get('name', tool_name)
                proc_warehouse = tool.get('warehouse', warehouse)

                # Build parameter signature for the procedure
                params = tool.get('parameters', [])

                # Tool resources: 'function' for UDFs, 'procedure' for stored procedures
                tool_resources[tool_name] = {
                    'type': 'procedure',
                    'identifier': f"{proc_db}.{proc_schema}.{proc_name}",
                    'execution_environment': {
                        'type': 'warehouse',
                        'warehouse': proc_warehouse,
                        'query_timeout': tool.get('timeout', 300)  # Longer timeout for ML
                    }
                }

                # Build input schema from parameters (required for generic tools)
                input_schema = {
                    'type': 'object',
                    'properties': {},
                    'required': []
                }
                for p in params:
                    input_schema['properties'][p['name']] = {
                        'type': 'string' if p['type'].upper() in ['TEXT', 'VARCHAR', 'STRING', 'DATE'] else 'number',
                        'description': p.get('description', '')
                    }
                    if p.get('required', True):
                        input_schema['required'].append(p['name'])

                tool_spec['tool_spec']['input_schema'] = input_schema

        # Build instructions
        instructions = self.agent_config.get('instructions', {})
        response_inst = instructions.get('response', '').strip()
        orch_inst = instructions.get('orchestration', '').strip()

        # Build sample questions
        sample_questions = [
            {'question': q}
            for q in self.agent_config.get('sample_questions', [])
        ]

        # Get orchestration model from environment config (default to auto)
        model_config = self.env_config.get('model', {})
        orchestration_model = model_config.get('orchestration', 'auto')

        # Final specification
        spec = {
            'models': {
                'orchestration': orchestration_model
            },
            'instructions': {
                'response': response_inst,
                'orchestration': orch_inst,
                'sample_questions': sample_questions
            },
            'tools': tools,
            'tool_resources': tool_resources
        }

        return spec

    def build_create_agent_sql(self) -> str:
        """Generate the CREATE AGENT SQL statement (single statement for execution)."""

        metadata = self.agent_config.get('metadata', {})
        profile = self.agent_config.get('profile', {})

        agent_name = metadata.get('name', self.agent_name).upper()
        version = metadata.get('version', '1.0.0')
        display_name = profile.get('display_name', agent_name)
        color = profile.get('color', 'blue')
        description = self.agent_config.get('description', '').strip()[:200]

        # Add environment suffix if configured
        suffix = self.env_config.get('settings', {}).get('version_suffix', '')
        if suffix:
            agent_name = f"{agent_name}{suffix.upper().replace('-', '_')}"

        # Store the final agent name for later use
        self._final_agent_name = agent_name

        # Build profile JSON
        profile_json = json.dumps({
            'display_name': display_name,
            'color': color
        })

        # Build specification
        spec = self.build_specification()
        spec_json = json.dumps(spec, indent=2)

        # Build single CREATE AGENT statement (no USE or SHOW - those are handled separately)
        sql = f"""CREATE OR REPLACE AGENT {agent_name}
  COMMENT = '{description}'
  PROFILE = '{profile_json}'
  FROM SPECIFICATION
  $$
{spec_json}
  $$"""
        return sql

    def build_full_sql_file(self) -> str:
        """Generate full SQL file with comments for documentation."""

        metadata = self.agent_config.get('metadata', {})
        version = metadata.get('version', '1.0.0')

        deployment = self.env_config.get('agent_deployment', {})
        db = deployment.get('database', 'SNOWFLAKE_INTELLIGENCE')
        schema = deployment.get('schema', 'AGENTS')

        create_sql = self.build_create_agent_sql()
        agent_name = self._final_agent_name

        full_sql = f"""
-- =============================================================================
-- Agent: {agent_name}
-- Version: {version}
-- Environment: {self.environment}
-- Generated: {datetime.now().isoformat()}
-- =============================================================================

USE DATABASE {db};
USE SCHEMA {schema};

{create_sql};

-- Verify agent was created
SHOW AGENTS LIKE '{agent_name}';
"""
        return full_sql

    def save_generated_sql(self) -> Path:
        """Save generated SQL (full file with comments) for audit trail."""

        metadata = self.agent_config.get('metadata', {})
        version = metadata.get('version', '1.0.0')

        # Generate the full SQL file with USE statements and comments
        full_sql = self.build_full_sql_file()

        filename = f"{self.agent_name}_{self.environment}_v{version}.sql"
        filepath = self.GENERATED_DIR / filename

        with open(filepath, 'w') as f:
            f.write(full_sql)

        return filepath

    def deploy(self) -> bool:
        """Deploy the agent to Snowflake."""

        logger.info("=" * 70)
        logger.info(f"DEPLOYING AGENT: {self.agent_name}")
        logger.info(f"Environment: {self.environment}")
        logger.info(f"Version: {self.agent_config.get('metadata', {}).get('version', 'unknown')}")
        logger.info("=" * 70)

        # Generate CREATE AGENT SQL (single statement)
        create_sql = self.build_create_agent_sql()

        # Save full SQL file for audit trail
        sql_file = self.save_generated_sql()
        logger.info(f"✓ Generated SQL saved to: {sql_file}")

        if self.dry_run:
            logger.info("\n[DRY RUN] Would execute:")
            print(create_sql)
            return True

        # Connect using SnowflakeConnection from data_generation
        connection_name = self.env_config.get('snowflake', {}).get('connection_name', 'snowflake_agents')

        try:
            # Use the existing SnowflakeConnection that handles Snow CLI config
            conn = SnowflakeConnection.from_snow_cli(connection_name)
            logger.info(f"✓ Connected via Snow CLI connection '{connection_name}'")

            # Setup database/schema (each as separate statement)
            deployment = self.env_config.get('agent_deployment', {})
            db = deployment.get('database', 'SNOWFLAKE_INTELLIGENCE')
            schema = deployment.get('schema', 'AGENTS')

            conn.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {db}.{schema}")
            conn.execute(f"USE DATABASE {db}")
            conn.execute(f"USE SCHEMA {schema}")

            # Execute CREATE AGENT (single statement)
            conn.execute(create_sql)

            logger.info(f"✓ Agent {self._final_agent_name} created in {db}.{schema}")

            # Add to Snowflake Intelligence if configured
            add_to_si = deployment.get('add_to_snowflake_intelligence', False)
            if add_to_si:
                self._add_to_snowflake_intelligence(conn, db, schema)

            # Log deployment
            self._log_deployment(conn, db, schema)

            conn.close()
            return True

        except Exception as e:
            logger.error(f"Deployment failed: {e}")
            logger.info(f"SQL saved to: {sql_file}")
            return False

    def _add_to_snowflake_intelligence(self, conn, db: str, schema: str):
        """Add agent to Snowflake Intelligence for UI visibility."""

        agent_fqn = f"{db}.{schema}.{self._final_agent_name}"

        try:
            # Check if Snowflake Intelligence object exists
            result = conn.fetch("SHOW SNOWFLAKE INTELLIGENCES")

            if result:
                # Add agent to Snowflake Intelligence
                conn.execute(f"""
                    ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT
                    ADD AGENT {agent_fqn}
                """)
                logger.info(f"✓ Agent added to Snowflake Intelligence")
            else:
                logger.info("ℹ️  No Snowflake Intelligence object exists - agent not added to SI")
                logger.info("   Agent is still accessible via SQL: SELECT * FROM TABLE(AGENT(...)")

        except Exception as e:
            # Don't fail deployment if SI addition fails
            logger.warning(f"⚠️  Could not add to Snowflake Intelligence: {e}")
            logger.info("   Agent was created successfully - you can add it manually:")
            logger.info(f"   ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT ADD AGENT {agent_fqn}")

    def _log_deployment(self, conn, db: str, schema: str):
        """Log deployment to tracking table."""

        metadata = self.agent_config.get('metadata', {})
        profile = self.agent_config.get('profile', {})

        try:
            conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {db}.{schema}.AGENT_DEPLOYMENTS (
                DEPLOYMENT_ID VARCHAR DEFAULT UUID_STRING(),
                AGENT_NAME VARCHAR,
                VERSION VARCHAR,
                ENVIRONMENT VARCHAR,
                DISPLAY_NAME VARCHAR,
                TOOL_COUNT NUMBER,
                DEPLOYED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                DEPLOYED_BY VARCHAR DEFAULT CURRENT_USER(),
                STATUS VARCHAR DEFAULT 'SUCCESS'
            )
            """)

            tool_count = len(self.agent_config.get('tools', []))

            conn.execute(f"""
            INSERT INTO {db}.{schema}.AGENT_DEPLOYMENTS
            (AGENT_NAME, VERSION, ENVIRONMENT, DISPLAY_NAME, TOOL_COUNT)
            VALUES (
                '{metadata.get('name', self.agent_name)}',
                '{metadata.get('version', '1.0.0')}',
                '{self.environment}',
                '{profile.get('display_name', '')}',
                {tool_count}
            )
            """)

            logger.info("✓ Deployment logged")

        except Exception as e:
            logger.warning(f"Could not log deployment: {e}")


def list_agents() -> list:
    """List available agent configurations."""
    agents_dir = Path(__file__).parent / 'agents'
    agents = []

    for f in agents_dir.glob('*.yml'):
        if f.stem not in ['environments']:
            agents.append(f.stem)

    return agents


def main():
    parser = argparse.ArgumentParser(
        description='Deploy Snowflake Intelligence Agents',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python deploy.py --agent ski_ops_assistant --env dev
  python deploy.py --agent customer_insights --env staging
  python deploy.py --all --env dev
  python deploy.py --agent resort_executive --env prod --dry-run
  python deploy.py --list
        """
    )

    parser.add_argument('--agent', '-a',
                       help='Agent name to deploy')
    parser.add_argument('--env', '-e', default='dev',
                       choices=['dev', 'staging', 'prod'],
                       help='Target environment (default: dev)')
    parser.add_argument('--all', action='store_true',
                       help='Deploy all agents')
    parser.add_argument('--dry-run', '-n', action='store_true',
                       help='Generate SQL without deploying')
    parser.add_argument('--list', '-l', action='store_true',
                       help='List available agents')

    args = parser.parse_args()

    if args.list:
        print("\nAvailable agents:")
        for agent in list_agents():
            print(f"  - {agent}")
        return

    if args.all:
        agents = list_agents()
    elif args.agent:
        agents = [args.agent]
    else:
        parser.error("Specify --agent or --all")
        return

    # Deploy each agent
    success_count = 0
    for agent in agents:
        try:
            deployer = AgentDeployer(agent, args.env, args.dry_run)
            if deployer.deploy():
                success_count += 1
        except Exception as e:
            logger.error(f"Failed to deploy {agent}: {e}")

    # Summary
    print(f"\n{'='*70}")
    print(f"DEPLOYMENT SUMMARY")
    print(f"{'='*70}")
    print(f"Total agents: {len(agents)}")
    print(f"Successful: {success_count}")
    print(f"Failed: {len(agents) - success_count}")
    print(f"Environment: {args.env}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'DEPLOYED'}")


if __name__ == "__main__":
    main()
