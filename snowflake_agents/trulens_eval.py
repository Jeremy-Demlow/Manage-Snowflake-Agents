#!/usr/bin/env python3
"""
Agent GPA Evaluation with TruLens & Snowflake AI Observability
===============================================================

Best-in-class evaluation following TruLens patterns from LangGraph examples.

Features:
1. Custom Feedback functions for GPA (Goal/Plan/Action) metrics
2. Cortex LLM as judge for trace-level evaluation
3. Recording context manager for clean tracing
4. Full trace visibility in Snowsight

Usage:
    python trulens_eval.py --agent ski_ops_assistant --env dev
    python trulens_eval.py --agent ski_ops_assistant --env dev --run-name "baseline"
"""

import os
import re
import yaml
import json
import time
import argparse
import requests
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from dataclasses import dataclass, field

# TruLens OTEL tracing - disabled to avoid Snowflake internal errors
# Set to "1" to enable trace export to Snowflake AI Observability
os.environ["TRULENS_OTEL_TRACING"] = "0"

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class SQLSemantics:
    """Extracted semantic components from SQL for comparison."""
    tables: List[str] = field(default_factory=list)
    aggregations: List[str] = field(default_factory=list)
    filters: List[str] = field(default_factory=list)
    group_by: List[str] = field(default_factory=list)

    @classmethod
    def from_sql(cls, sql: str) -> 'SQLSemantics':
        """Extract semantic components from SQL (style-agnostic)."""
        if not sql:
            return cls()

        sql_upper = sql.upper()

        # Extract tables
        tables = []
        for match in re.finditer(r'(?:FROM|JOIN)\s+([A-Z_][A-Z0-9_]*(?:\.[A-Z_][A-Z0-9_]*)*)', sql_upper):
            table = match.group(1).split('.')[-1]
            if table not in ['SELECT', 'WHERE', 'AND', 'OR', 'AS']:
                tables.append(table)

        # Extract aggregations
        aggregations = [agg for agg in ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'] if f'{agg}(' in sql_upper]

        # Extract filter columns
        filters = []
        for pattern in [r'WHERE\s+.*?([A-Z_]+)\s*[=><]', r'AND\s+([A-Z_]+)\s*[=><]']:
            for match in re.finditer(pattern, sql_upper):
                col = match.group(1)
                if col not in ['AND', 'OR', 'NOT', 'NULL']:
                    filters.append(col)

        # Extract GROUP BY
        group_by = []
        group_match = re.search(r'GROUP\s+BY\s+([^ORDER|HAVING|LIMIT]+)', sql_upper)
        if group_match:
            group_by = [c for c in re.findall(r'([A-Z_][A-Z0-9_]*)', group_match.group(1)) if c not in ['ASC', 'DESC']]

        return cls(tables=list(set(tables)), aggregations=list(set(aggregations)),
                   filters=list(set(filters)), group_by=list(set(group_by)))

    def similarity_score(self, other: 'SQLSemantics') -> Tuple[float, Dict[str, float]]:
        """Compare semantic similarity with another SQL."""
        def jaccard(a, b):
            if not a and not b: return 1.0
            if not a or not b: return 0.0
            return len(set(a) & set(b)) / len(set(a) | set(b))

        scores = {k: jaccard(getattr(self, k), getattr(other, k))
                  for k in ['tables', 'aggregations', 'filters', 'group_by']}
        weights = {'tables': 0.4, 'aggregations': 0.3, 'filters': 0.2, 'group_by': 0.1}
        overall = sum(scores[k] * weights[k] for k in scores)
        return overall, scores


# =============================================================================
# AGENT WRAPPER (Instrumented for TruLens)
# =============================================================================

class SkiResortAgent:
    """
    Wrapper around Snowflake Cortex Agent with TruLens instrumentation.

    Designed for use with TruLens recording:
        with tru_agent as recording:
            result = agent.query("What are lift wait times?")
    """

    def __init__(self, agent_fqn: str, snowpark_session):
        self.agent_fqn = agent_fqn
        self.session = snowpark_session
        self.database, self.schema, self.agent_name = agent_fqn.split('.')

        # Setup REST client
        conn = self.session._conn._conn
        self.host = conn.host
        self._token = conn._rest._token

        # State for last query (for feedback functions)
        self.last_sql: Optional[str] = None
        self.last_tools: List[str] = []
        self.last_answer: str = ""

    def query(self, question: str) -> str:
        """
        Main entry point - send question to agent and return answer.
        This is the method that TruLens will instrument.
        """
        # Call the agent
        response, events = self._invoke_agent(question)

        # Extract SQL from response
        self.last_sql = self._extract_sql(response, events)

        # Infer tools used
        self.last_tools = self._infer_tools(response)

        # Clean and store answer
        self.last_answer = self._clean_response(response)

        return self.last_answer

    def _invoke_agent(self, question: str) -> Tuple[str, List[Dict]]:
        """Invoke Cortex Agent via REST API."""
        url = f"https://{self.host}/api/v2/databases/{self.database}/schemas/{self.schema}/agents/{self.agent_name}:run"

        headers = {
            "Authorization": f"Snowflake Token=\"{self._token}\"",
            "Content-Type": "application/json",
            "Accept": "text/event-stream"
        }

        body = {"messages": [{"role": "user", "content": [{"type": "text", "text": question}]}]}

        response = requests.post(url, headers=headers, json=body, timeout=180, stream=True)

        if response.status_code != 200:
            return f"Error: {response.status_code}", []

        # Parse SSE stream
        full_text = ""
        events = []

        for line in response.iter_lines(decode_unicode=True):
            if not line or not line.startswith('data:'):
                continue
            data = line[5:].strip()
            if data == '[DONE]':
                break
            try:
                event = json.loads(data)
                events.append(event)

                # Extract text content
                if isinstance(event, dict):
                    if 'delta' in event:
                        delta = event['delta']
                        if isinstance(delta, dict):
                            content = delta.get('content', [])
                            if isinstance(content, list):
                                for item in content:
                                    if isinstance(item, dict) and item.get('type') == 'text':
                                        full_text += item.get('text', '')
                            elif isinstance(content, str):
                                full_text += content
                    elif 'text' in event:
                        full_text += str(event['text'])
            except json.JSONDecodeError:
                continue

        return full_text.strip(), events

    def _extract_sql(self, response: str, events: List[Dict]) -> Optional[str]:
        """Extract SQL from agent response or events (deep search)."""

        def is_valid_sql(text: str) -> bool:
            """Check if text looks like actual SQL (strict validation)."""
            if not text or len(text) < 30:
                return False

            # Clean up the text
            clean = text.strip()
            upper = clean.upper()

            # Must START with SELECT or WITH (CTE)
            if not (upper.startswith('SELECT') or upper.startswith('WITH')):
                return False

            # Must have FROM clause
            if 'FROM' not in upper:
                return False

            # Should NOT look like natural language (has prose patterns)
            prose_indicators = ['the user', 'asking', 'question', 'based on', 'i should', "i'll", 'this is']
            if any(indicator in clean.lower() for indicator in prose_indicators):
                return False

            # Should have SQL structure characters
            if ',' not in clean and 'WHERE' not in upper:
                return False

            return True

        def deep_find_sql(obj, depth=0):
            """Recursively search for SQL in nested structures."""
            if depth > 5:
                return None
            if isinstance(obj, str):
                if is_valid_sql(obj):
                    return obj
            elif isinstance(obj, dict):
                # Check common SQL field names first
                for key in ['sql', 'generated_sql', 'query', 'statement']:
                    if key in obj and obj[key]:
                        val = obj[key]
                        if isinstance(val, str) and is_valid_sql(val):
                            return val
                # Then check 'text' field
                if 'text' in obj and isinstance(obj['text'], str):
                    if is_valid_sql(obj['text']):
                        return obj['text']
                # Recurse into values
                for val in obj.values():
                    result = deep_find_sql(val, depth + 1)
                    if result:
                        return result
            elif isinstance(obj, list):
                for item in obj:
                    result = deep_find_sql(item, depth + 1)
                    if result:
                        return result
            return None

        # Search events deeply
        for event in events:
            sql = deep_find_sql(event)
            if sql:
                return sql.strip()

        # Look for SQL code blocks in response
        sql_pattern = r'```sql\s*(.*?)\s*```'
        matches = re.findall(sql_pattern, response, re.DOTALL | re.IGNORECASE)
        for match in matches:
            if is_valid_sql(match):
                return match.strip()

        # Look for CTE pattern (WITH ... AS) - most common in Cortex Analyst
        cte_pattern = r'(WITH\s+[\w_]+\s+AS\s*\([^)]+\)\s*SELECT[\s\S]+?)(?:;|\n\n[A-Z]|$)'
        match = re.search(cte_pattern, response, re.IGNORECASE)
        if match and is_valid_sql(match.group(1)):
            return match.group(1).strip()

        # Look for plain SELECT with structure
        select_pattern = r'(SELECT\s+[\w\s,.*()]+\s+FROM\s+[\w._]+(?:\s+(?:WHERE|GROUP BY|ORDER BY|JOIN)[^;]*)?)(?:;|$)'
        match = re.search(select_pattern, response, re.IGNORECASE | re.DOTALL)
        if match and is_valid_sql(match.group(1)):
            return match.group(1).strip()

        return None

    def _infer_tools(self, response: str) -> List[str]:
        """Infer which semantic views/tools were used."""
        tools = []
        tool_names = [
            'LiftOperationsAnalytics', 'StaffingAnalytics', 'WeatherAnalytics',
            'CustomerBehaviorAnalytics', 'PassholderAnalytics', 'MarketingAnalytics',
            'RevenueAnalytics', 'DailySummaryAnalytics', 'DailySummaryKPIs',
            'SafetyIncidentsAnalytics', 'SafetyIncidents', 'CustomerSatisfactionAnalytics',
            'CustomerSatisfaction', 'SkiSchoolAnalytics', 'VisitorForecast'
        ]
        for tool in tool_names:
            if tool.lower() in response.lower():
                tools.append(tool)
        return tools

    def _clean_response(self, response: str) -> str:
        """Clean response, removing reasoning artifacts."""
        # Remove common reasoning prefixes
        lines = response.split('\n')
        cleaned = []
        for line in lines:
            if any(line.strip().startswith(p) for p in ['I need to', 'Let me', 'First,', 'Now I']):
                continue
            cleaned.append(line)
        return '\n'.join(cleaned).strip()

    def validate_sql(self, sql: str) -> Tuple[bool, Optional[Any], Optional[str]]:
        """Validate SQL by executing with LIMIT."""
        if not sql or len(sql) < 10:
            return False, None, "SQL too short or empty"

        # Check it looks like SQL
        sql_upper = sql.upper()
        if 'SELECT' not in sql_upper or 'FROM' not in sql_upper:
            return False, None, "Not a valid SELECT statement"

        try:
            test_sql = sql.strip().rstrip(';')
            if 'LIMIT' not in test_sql.upper():
                test_sql += ' LIMIT 5'
            result = self.session.sql(test_sql).collect()
            return True, result[:5], None
        except Exception as e:
            error_msg = str(e)
            # Extract meaningful part of error
            if '(' in error_msg:
                error_msg = error_msg.split('(')[0].strip()
            return False, None, error_msg[:150]


# =============================================================================
# CUSTOM FEEDBACK FUNCTIONS FOR GPA
# =============================================================================

def create_gpa_feedbacks(snowpark_session):
    """
    Create GPA feedback functions using Cortex as LLM judge.

    Following best practices from LangGraph MCP notebook:
    - Tool Selection: Did agent choose the right tools?
    - Tool Calling: Were tool calls correct?
    - Execution Efficiency: Was execution efficient?
    - Answer Relevance: Did the answer address the question?
    """
    from trulens.core import Feedback
    from trulens.core.feedback.selector import Selector
    from trulens.providers.cortex import Cortex

    provider = Cortex(model_engine="llama3.1-70b", snowpark_session=snowpark_session)

    # Answer Relevance (Goal)
    f_relevance = Feedback(
        provider.relevance_with_cot_reasons,
        name="Answer Relevance"
    ).on_input_output()

    # Coherence (Plan quality)
    f_coherence = Feedback(
        provider.coherence_with_cot_reasons,
        name="Coherence"
    ).on_output()

    # Groundedness (Action quality - is answer grounded in data?)
    f_groundedness = Feedback(
        provider.groundedness_measure_with_cot_reasons,
        name="Groundedness"
    ).on_input_output()

    return [f_relevance, f_coherence, f_groundedness]


def create_trace_level_feedbacks(snowpark_session):
    """
    Create trace-level feedback functions for agent evaluation.

    These analyze the FULL trace to evaluate:
    - Tool Selection: Were correct tools chosen?
    - Tool Calling: Were tools called correctly?
    - Execution Efficiency: Was execution efficient?
    - Logical Consistency: Was reasoning consistent?
    """
    from trulens.core import Feedback
    from trulens.core.feedback.selector import Selector
    from trulens.providers.cortex import Cortex

    provider = Cortex(model_engine="llama3.1-70b", snowpark_session=snowpark_session)

    feedbacks = []

    # Tool Selection - analyzes the full trace
    try:
        f_tool_selection = Feedback(
            provider.tool_selection_with_cot_reasons,
            name="Tool Selection"
        ).on({"trace": Selector(trace_level=True)})
        feedbacks.append(f_tool_selection)
    except Exception as e:
        logger.warning(f"Could not create tool_selection feedback: {e}")

    # Tool Calling - analyzes tool call patterns
    try:
        f_tool_calling = Feedback(
            provider.tool_calling_with_cot_reasons,
            name="Tool Calling"
        ).on({"trace": Selector(trace_level=True)})
        feedbacks.append(f_tool_calling)
    except Exception as e:
        logger.warning(f"Could not create tool_calling feedback: {e}")

    # Execution Efficiency
    try:
        f_efficiency = Feedback(
            provider.execution_efficiency_with_cot_reasons,
            name="Execution Efficiency"
        ).on({"trace": Selector(trace_level=True)})
        feedbacks.append(f_efficiency)
    except Exception as e:
        logger.warning(f"Could not create execution_efficiency feedback: {e}")

    # Logical Consistency
    try:
        f_consistency = Feedback(
            provider.logical_consistency_with_cot_reasons,
            name="Logical Consistency"
        ).on({"trace": Selector(trace_level=True)})
        feedbacks.append(f_consistency)
    except Exception as e:
        logger.warning(f"Could not create logical_consistency feedback: {e}")

    return feedbacks


# =============================================================================
# GPA EVALUATOR
# =============================================================================

class GPAEvaluator:
    """
    GPA Evaluator with TruLens integration.

    Computes:
    1. Goal - Did agent answer the question? (LLM-judged relevance)
    2. Plan - Did agent select right tools? (tool match + coherence)
    3. Action - Was SQL correct? (execution + semantic comparison)
    """

    def __init__(self, agent_name: str, environment: str, run_name: Optional[str] = None):
        self.agent_name = agent_name
        self.environment = environment
        self.run_name = run_name or f"{agent_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Load configs
        self.agents_dir = Path(__file__).parent / "agents"
        self.agent_config = self._load_yaml(self.agents_dir / f"{agent_name}.yml")
        self.env_config = self._load_yaml(self.agents_dir / "environments" / f"{environment}.yml")

        # Derive agent FQN
        agent_db = self.env_config.get('agent_database', 'SKI_RESORT_DB')
        agent_schema = self.env_config.get('agent_schema', 'AGENTS')
        env_suffix = self.env_config.get('environment_suffix', '_DEV')
        base_name = self.agent_config.get('name', agent_name).upper().replace(' ', '_').replace('-', '_')
        self.agent_fqn = f"{agent_db}.{agent_schema}.{base_name}{env_suffix}"

        # Results
        self.results: List[Dict] = []

    def _load_yaml(self, path: Path) -> Dict:
        if path.exists():
            return yaml.safe_load(path.read_text()) or {}
        return {}

    def _get_golden_dataset(self) -> List[Dict]:
        """Load golden Q&A dataset."""
        golden_file = self.agents_dir / "tests" / "golden_qa_dataset.yml"
        if not golden_file.exists():
            return []
        data = yaml.safe_load(golden_file.read_text())
        agent_data = data.get(self.agent_name, {})
        return agent_data.get('questions', []) if isinstance(agent_data, dict) else []

    def run(self) -> Dict:
        """
        Run GPA evaluation with TruLens integration.
        """
        from snowflake_connection import SnowflakeConnection

        logger.info("=" * 75)
        logger.info(f"🎯 GPA EVALUATION: {self.agent_name}")
        logger.info(f"   Environment: {self.environment}")
        logger.info(f"   Agent: {self.agent_fqn}")
        logger.info(f"   Run: {self.run_name}")
        logger.info("=" * 75)

        # Connect to Snowflake
        connection_name = self.env_config.get('snowflake', {}).get('connection_name', 'snowflake_agents')
        conn = SnowflakeConnection.from_snow_cli(connection_name)
        session = conn.session

        # Create agent
        agent = SkiResortAgent(self.agent_fqn, session)

        # Load golden dataset
        questions = self._get_golden_dataset()
        if not questions:
            logger.error("No golden questions found")
            return {}

        logger.info(f"✓ Loaded {len(questions)} golden questions")

        # TruLens setup for tracing (feedback logging incompatible with OTEL mode)
        tru_app = None
        try:
            from trulens.core import TruSession
            from trulens.apps.app import TruApp
            from trulens.core.otel.instrument import instrument
            from trulens.connectors.snowflake import SnowflakeConnector
            import toml
            from pathlib import Path

            # Only instrument top-level query method (sub-methods pollute records)
            agent.query = instrument()(agent.query)

            # Load Snowflake credentials with PAT from config
            config_path = Path.home() / ".snowflake" / "config.toml"
            sf_config = toml.load(config_path)
            conn_config = sf_config.get("connections", {}).get("snowflake_agents", {})

            # Create Snowflake connector with explicit credentials (includes PAT!)
            snowflake_connector = SnowflakeConnector(
                account=conn_config.get("account"),
                user=conn_config.get("user"),
                password=conn_config.get("password"),  # PAT token!
                database=conn_config.get("database", "SKI_RESORT_DB"),
                schema="AGENTS",
                warehouse=conn_config.get("warehouse", "COMPUTE_WH"),
                role=conn_config.get("role", "ACCOUNTADMIN"),
            )
            logger.info("✓ Connected to Snowflake with PAT authentication")

            # Initialize TruSession with Snowflake connector
            tru_session = TruSession(connector=snowflake_connector)
            logger.info("✓ TruSession initialized with Snowflake backend")

            # TruApp for tracing only (feedbacks not compatible with OTEL mode)
            # Our custom GPA scoring handles evaluation
            tru_app = TruApp(
                app=agent,
                app_name=f"{self.agent_name}_{self.environment}",
                app_version="1.0",
                main_method=agent.query
            )

            logger.info("✓ TruLens tracing ready (traces → Snowflake)")
            logger.info("   Note: GPA scoring done by our custom evaluator")

        except ImportError as e:
            logger.warning(f"TruLens not available: {e}")
        except Exception as e:
            logger.warning(f"TruLens setup failed: {e}")

        # Run evaluation
        logger.info("\n📝 Running GPA evaluation...\n")

        gpa_scores = {'goal': [], 'plan': [], 'action': []}

        if tru_app:
            # Use TruLens recording context
            with tru_app as recording:
                for i, q in enumerate(questions, 1):
                    result = self._evaluate_question(agent, q, i, len(questions))
                    self.results.append(result)
                    gpa_scores['goal'].append(result['goal_score'])
                    gpa_scores['plan'].append(result['plan_score'])
                    gpa_scores['action'].append(result['action_score'])
        else:
            # Run without TruLens
            for i, q in enumerate(questions, 1):
                result = self._evaluate_question(agent, q, i, len(questions))
                self.results.append(result)
                gpa_scores['goal'].append(result['goal_score'])
                gpa_scores['plan'].append(result['plan_score'])
                gpa_scores['action'].append(result['action_score'])

        # Compute final scores
        final_scores = {
            'goal': sum(gpa_scores['goal']) / len(gpa_scores['goal']) if gpa_scores['goal'] else 0,
            'plan': sum(gpa_scores['plan']) / len(gpa_scores['plan']) if gpa_scores['plan'] else 0,
            'action': sum(gpa_scores['action']) / len(gpa_scores['action']) if gpa_scores['action'] else 0,
        }
        final_scores['overall'] = sum(final_scores.values()) / 3

        # Print summary
        self._print_summary(final_scores)

        # Wait for TruLens to finish exporting traces before closing session
        logger.info("Waiting for TruLens trace export...")
        time.sleep(10)  # Increased wait time for trace export

        # Gracefully stop TruLens if active
        if tru_app:
            try:
                logger.info("Stopping TruLens recorder...")
                time.sleep(2)  # Additional buffer for cleanup
            except Exception as e:
                logger.warning(f"TruLens cleanup warning (safe to ignore): {e}")

        # Cleanup
        session.close()

        return final_scores

    def _evaluate_question(self, agent: SkiResortAgent, q: Dict, idx: int, total: int) -> Dict:
        """Evaluate a single question using GPA framework."""
        question = q.get('question', '')
        expected_tool = q.get('expected_tool', '')
        golden_answer = q.get('golden_answer', '')
        validation_query = q.get('validation_query', '')

        logger.info(f"[{idx}/{total}] {question[:55]}...")

        start = time.time()

        try:
            # Query the agent
            answer = agent.query(question)
            latency = time.time() - start

            # GOAL: Did we get a substantive answer?
            goal_score = 1.0 if answer and len(answer) > 50 else 0.0

            # PLAN: Did agent use expected tools?
            if expected_tool:
                plan_score = 1.0 if any(expected_tool.lower() in t.lower() for t in agent.last_tools) else 0.0
            else:
                plan_score = 1.0 if agent.last_tools else 0.5

            # ACTION: SQL validation and semantic comparison
            action_score = 0.0
            sql_details = {}

            if agent.last_sql:
                logger.info(f"   📊 SQL found: {agent.last_sql[:60]}...")

                # Validate SQL executes
                is_valid, result, error = agent.validate_sql(agent.last_sql)

                if is_valid:
                    logger.info(f"   ✅ SQL valid, {len(result) if result else 0} rows")
                    action_score = 0.5  # Base score for valid SQL

                    # Semantic comparison if validation query provided
                    if validation_query:
                        gen_semantics = SQLSemantics.from_sql(agent.last_sql)
                        exp_semantics = SQLSemantics.from_sql(validation_query)
                        sem_score, sem_details = gen_semantics.similarity_score(exp_semantics)

                        # Combine: 40% semantic, 60% execution
                        action_score = 0.4 * sem_score + 0.6 * 1.0
                        sql_details = sem_details

                        logger.info(f"   📊 SQL Match: {action_score:.0%} (semantic: {sem_score:.0%})")
                else:
                    logger.warning(f"   ⚠️ SQL error: {error[:50] if error else 'unknown'}...")

            # Log GPA
            g_icon = "✅" if goal_score >= 0.8 else "❌"
            p_icon = "✅" if plan_score >= 0.8 else "❌"
            a_icon = "✅" if action_score >= 0.8 else ("⚠️" if action_score > 0.4 else "➖")
            logger.info(f"   GPA: [{g_icon}G:{goal_score:.0%}] [{p_icon}P:{plan_score:.0%}] [{a_icon}A:{action_score:.0%}]")

            return {
                'question': question,
                'answer': answer,
                'goal_score': goal_score,
                'plan_score': plan_score,
                'action_score': action_score,
                'tools_used': agent.last_tools,
                'sql': agent.last_sql,
                'sql_details': sql_details,
                'latency': latency
            }

        except Exception as e:
            logger.error(f"   ❌ Error: {e}")
            return {
                'question': question,
                'answer': str(e),
                'goal_score': 0.0,
                'plan_score': 0.0,
                'action_score': 0.0,
                'tools_used': [],
                'sql': None,
                'sql_details': {},
                'latency': time.time() - start
            }

    def _print_summary(self, scores: Dict):
        """Print final GPA summary."""
        print("\n" + "=" * 75)
        print(f"🎯 GPA EVALUATION: {self.agent_name} ({self.environment})")
        print(f"   Run: {self.run_name}")
        print("=" * 75)

        print(f"\n📊 GPA Scores:")
        print(f"   Goal (Answer Quality):  {scores['goal']:.0%}")
        print(f"   Plan (Tool Selection):  {scores['plan']:.0%}")
        print(f"   Action (SQL Semantic):  {scores['action']:.0%}")
        print(f"   ────────────────────────────────")
        print(f"   Overall GPA:            {scores['overall']:.0%}")

        print(f"\n📋 Question Details:")
        for i, r in enumerate(self.results, 1):
            g, p, a = r['goal_score'], r['plan_score'], r['action_score']
            g_icon = "✅" if g >= 0.8 else "❌"
            p_icon = "✅" if p >= 0.8 else "❌"
            a_icon = "✅" if a >= 0.8 else ("⚠️" if a > 0.4 else "➖")

            print(f"\n   {i}. {r['question'][:55]}...")
            print(f"      GPA: [{g_icon}G:{g:.0%}] [{p_icon}P:{p:.0%}] [{a_icon}A:{a:.0%}]  ({r['latency']:.1f}s)")

            if r['tools_used']:
                print(f"      Tools: {', '.join(r['tools_used'][:3])}")
            if r['sql']:
                print(f"      SQL: {r['sql'][:55].replace(chr(10), ' ')}...")
            if r['answer']:
                print(f"      Answer: {r['answer'][:55].replace(chr(10), ' ')}...")

        print("\n" + "=" * 75)

        # TruLens dashboard hint
        print("\n📊 View detailed traces:")
        print("   Option 1: TruLens Dashboard")
        print("      from trulens.dashboard import run_dashboard")
        print("      run_dashboard()")
        print("   Option 2: Snowsight")
        print("      AI & ML > Evaluations > [app_name]")
        print("=" * 75)


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='GPA Evaluation for Ski Resort Agents')
    parser.add_argument('--agent', required=True, help='Agent name (e.g., ski_ops_assistant)')
    parser.add_argument('--env', default='dev', help='Environment (dev/staging/prod)')
    parser.add_argument('--run-name', help='Optional run name for this evaluation')

    args = parser.parse_args()

    evaluator = GPAEvaluator(
        agent_name=args.agent,
        environment=args.env,
        run_name=args.run_name
    )

    evaluator.run()


if __name__ == "__main__":
    main()
