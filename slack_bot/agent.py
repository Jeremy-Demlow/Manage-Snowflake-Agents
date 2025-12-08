"""
Cortex Agent Client for Slack Bot

Simplified agent client optimized for Slack bot use cases:
- PAT authentication (simplest for external bots)
- Multi-turn conversations
- Progress callbacks for UI updates
- SQL and chart extraction for display

Usage:
    from agent import AgentClient

    client = AgentClient.from_config(config['agents']['default'])
    result = client.ask("What is total revenue?", progress_callback=update_ui)
"""

import json
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import requests


@dataclass
class AgentResult:
    """Result from agent call with all extracted data."""

    answer: str = ""
    sql: Optional[str] = None
    result_set: Optional[Dict] = None
    column_names: List[str] = field(default_factory=list)
    chart_specs: List[Dict] = field(default_factory=list)
    tools_used: List[str] = field(default_factory=list)
    thinking: str = ""
    raw_events: List[Dict] = field(default_factory=list)
    duration_seconds: float = 0.0

    @property
    def has_data(self) -> bool:
        return bool(self.result_set and self.result_set.get("data"))

    @property
    def has_chart(self) -> bool:
        return bool(self.chart_specs)

    @property
    def has_sql(self) -> bool:
        return bool(self.sql and self.sql != "SQL query executed")


class AgentClient:
    """
    Cortex Agent REST API client for Slack bot.

    Handles streaming responses, extracts SQL/charts, supports multi-turn.
    """

    def __init__(
        self,
        agent_name: str,
        database: str,
        schema: str,
        account: str,
        pat: str,
        timeout: int = 120,
    ):
        self.agent_name = agent_name
        self.database = database
        self.schema = schema
        self.account = account.lower()
        self.pat = pat
        self.timeout = timeout

    @classmethod
    def from_config(
        cls, agent_config: Dict, account: str = None, pat: str = None
    ) -> "AgentClient":
        """
        Create client from config dict.

        Args:
            agent_config: Dict with name, database, schema keys
            account: Snowflake account (or SNOWFLAKE_ACCOUNT env var)
            pat: Personal Access Token (or SNOWFLAKE_PAT env var)
        """
        return cls(
            agent_name=agent_config["name"],
            database=agent_config["database"],
            schema=agent_config["schema"],
            account=account or os.getenv("SNOWFLAKE_ACCOUNT", ""),
            pat=pat or os.getenv("SNOWFLAKE_PAT", ""),
            timeout=agent_config.get("timeout", 120),
        )

    @classmethod
    def from_env(cls, agent_name: str, database: str, schema: str) -> "AgentClient":
        """
        Create client using environment variables for auth.

        Expects:
        - SNOWFLAKE_ACCOUNT
        - SNOWFLAKE_PAT
        """
        return cls(
            agent_name=agent_name,
            database=database,
            schema=schema,
            account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
            pat=os.getenv("SNOWFLAKE_PAT", ""),
        )

    @property
    def endpoint(self) -> str:
        """REST API endpoint."""
        host = f"{self.account}.snowflakecomputing.com"
        return f"https://{host}/api/v2/databases/{self.database}/schemas/{self.schema}/agents/{self.agent_name}:run"

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.pat}",
            "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
        }

    def _build_messages(
        self, question: str, conversation_history: List[Dict] = None
    ) -> List[Dict]:
        """Build messages array for multi-turn conversations."""
        messages = []

        if conversation_history:
            messages.extend(conversation_history)

        messages.append(
            {"role": "user", "content": [{"type": "text", "text": question}]}
        )

        return messages

    def ask(
        self,
        question: str,
        conversation_history: List[Dict] = None,
        progress_callback: Callable[[str], None] = None,
    ) -> AgentResult:
        """
        Ask the agent a question.

        Args:
            question: Natural language question
            conversation_history: Previous messages for multi-turn
            progress_callback: Optional callback for progress updates

        Returns:
            AgentResult with answer, SQL, charts, etc.
        """
        start_time = time.time()

        payload = {"messages": self._build_messages(question, conversation_history)}

        try:
            response = requests.post(
                self.endpoint,
                headers=self._headers(),
                json=payload,
                stream=True,
                timeout=self.timeout,
            )
            response.raise_for_status()

            result = self._parse_response(response, progress_callback)
            result.duration_seconds = time.time() - start_time

            return result

        except requests.RequestException as e:
            return AgentResult(
                answer=f"Error calling agent: {str(e)}",
                duration_seconds=time.time() - start_time,
            )

    def _parse_response(
        self,
        response: requests.Response,
        progress_callback: Callable[[str], None] = None,
    ) -> AgentResult:
        """Parse SSE stream and extract all useful data."""
        result = AgentResult()
        current_event = None
        last_status = None

        for line in response.iter_lines(decode_unicode=True):
            if not line:
                continue

            if line.startswith("event:"):
                current_event = line[6:].strip()
                continue

            if not line.startswith("data:"):
                continue

            data_str = line[5:].strip()
            if data_str == "[DONE]":
                break

            try:
                data = json.loads(data_str)
                result.raw_events.append(data)

                # Progress updates
                if progress_callback and "status" in data and "message" in data:
                    status = data["message"]
                    if status != last_status:
                        progress_callback(status)
                        last_status = status

                # Text answer (streaming deltas)
                if current_event == "response.text.delta" and "text" in data:
                    result.answer += data["text"]

                # Thinking/reasoning
                if current_event == "response.thinking.delta" and "text" in data:
                    result.thinking += data["text"]

                # Chart specifications
                if current_event == "response.chart":
                    chart_spec_str = data.get("chart_spec")
                    if chart_spec_str:
                        try:
                            chart_spec = json.loads(chart_spec_str)
                            result.chart_specs.append(
                                {
                                    "spec": chart_spec,
                                    "tool_use_id": data.get("tool_use_id"),
                                    "type": chart_spec.get("mark", "unknown"),
                                }
                            )
                        except json.JSONDecodeError:
                            pass

                # Tool results with SQL and result_set
                if current_event == "response.tool_result":
                    self._extract_tool_result(data, result)

                # Table data
                if current_event == "response.table":
                    self._extract_table_data(data, result)

                # Track tools used
                if "type" in data and "cortex" in str(data.get("type", "")).lower():
                    tool = data["type"]
                    if tool not in result.tools_used:
                        result.tools_used.append(tool)

                # Execution trace (backup SQL extraction)
                if current_event == "execution_trace":
                    self._extract_from_trace(data, result)

                # Final response
                if current_event == "response" and "content" in data:
                    for item in data["content"]:
                        if item.get("type") == "text":
                            result.answer = item.get("text", result.answer)

            except json.JSONDecodeError:
                continue

        return result

    def _extract_tool_result(self, data: Dict, result: AgentResult):
        """Extract SQL and result_set from tool_result event."""
        tool_type = data.get("type", "") or data.get("tool_type", "")

        if "cortex_analyst" not in tool_type:
            return

        for item in data.get("content", []):
            if not isinstance(item, dict) or "json" not in item:
                continue

            json_data = item["json"]

            # SQL
            if "sql" in json_data and not result.sql:
                result.sql = json_data["sql"]

            # Result set
            if "result_set" in json_data:
                result.result_set = json_data["result_set"]
                metadata = json_data["result_set"].get("resultSetMetaData", {})
                result.column_names = [
                    col["name"] for col in metadata.get("rowType", [])
                ]

    def _extract_table_data(self, data: Dict, result: AgentResult):
        """Extract data from response.table event."""
        if "result_set" in data:
            result.result_set = data["result_set"]
            metadata = data["result_set"].get("resultSetMetaData", {})
            result.column_names = [col["name"] for col in metadata.get("rowType", [])]

    def _extract_from_trace(self, data: Dict, result: AgentResult):
        """Extract SQL from execution_trace event (backup)."""
        if not isinstance(data, list):
            return

        for trace_item in data:
            if not isinstance(trace_item, str):
                continue
            try:
                trace_json = json.loads(trace_item)
                for attr in trace_json.get("attributes", []):
                    if (
                        attr.get("key")
                        == "snow.ai.observability.agent.tool.cortex_analyst.sql_query"
                    ):
                        sql = attr.get("value", {}).get("stringValue")
                        if sql and not result.sql:
                            result.sql = sql
            except json.JSONDecodeError:
                pass


# Simple API for quick usage
def ask(question: str, agent_name: str = None) -> str:
    """
    Quick helper to ask a question.

    Uses environment variables:
    - SNOWFLAKE_ACCOUNT
    - SNOWFLAKE_PAT
    - AGENT_NAME (or pass agent_name)
    - AGENT_DATABASE
    - AGENT_SCHEMA
    """
    client = AgentClient(
        agent_name=agent_name or os.getenv("AGENT_NAME", "RESORT_EXECUTIVE_DEV"),
        database=os.getenv("AGENT_DATABASE", "SKI_RESORT_DB"),
        schema=os.getenv("AGENT_SCHEMA", "AGENTS"),
        account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
        pat=os.getenv("SNOWFLAKE_PAT", ""),
    )
    result = client.ask(question)
    return result.answer


if __name__ == "__main__":
    # Quick test
    print("Testing agent client...")
    answer = ask("Hello, are you working?")
    print(f"Response: {answer[:200]}...")
