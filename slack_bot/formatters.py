"""
Slack Message Formatters

Clean, reusable formatters for Slack Block Kit messages.
Handles text, tables, SQL, charts, and metadata.

Usage:
    from formatters import SlackFormatter

    formatter = SlackFormatter()
    blocks = formatter.format_response(result, question)
"""

import csv
from io import BytesIO, StringIO
from typing import Any, Dict, List, Optional

from agent import AgentResult


class SlackFormatter:
    """
    Format agent responses for Slack using Block Kit.

    Produces clean, readable messages with:
    - Formatted answer text
    - Data tables (up to 15 rows)
    - SQL code blocks
    - Timing and tool metadata
    """

    def __init__(
        self,
        max_response_length: int = 2500,
        max_table_rows: int = 15,
        show_sql: bool = True,
        show_timing: bool = True,
    ):
        self.max_response_length = max_response_length
        self.max_table_rows = max_table_rows
        self.show_sql = show_sql
        self.show_timing = show_timing

    def format_response(
        self, result: AgentResult, question: str = "", emoji: str = "🎿"
    ) -> List[Dict[str, Any]]:
        """
        Format complete agent response as Slack blocks.

        Args:
            result: AgentResult from agent call
            question: Original question (for context)
            emoji: Emoji for this agent type

        Returns:
            List of Slack Block Kit blocks
        """
        blocks = []

        # Main answer
        answer = self._clean_answer(result.answer)
        blocks.append(self._text_block(answer))

        # Data table if present
        if result.has_data:
            table_block = self._format_table(result)
            if table_block:
                blocks.append({"type": "divider"})
                blocks.extend(table_block)

        # SQL if present
        if self.show_sql and result.has_sql:
            sql_block = self._format_sql(result.sql)
            if sql_block:
                blocks.append({"type": "divider"})
                blocks.append(sql_block)

        # Footer with metadata
        if self.show_timing:
            blocks.append(self._format_footer(result, emoji))

        return blocks

    def format_question_header(
        self, question: str, user_id: str, emoji: str = "💬"
    ) -> List[Dict[str, Any]]:
        """Format the question header block."""
        return [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"{emoji} New Question"},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Asked by:* <@{user_id}>\n*Question:* {question}",
                },
            },
        ]

    def format_progress(self, status: str) -> str:
        """Format a progress status message."""
        emoji_map = {
            "planning": "🧠",
            "executing": "⚡",
            "generating": "✨",
            "forming": "📝",
            "running": "🔧",
            "streaming": "📊",
            "analyzing": "🔍",
        }

        status_lower = status.lower()
        for key, emoji in emoji_map.items():
            if key in status_lower:
                return f"{emoji} {status}..."

        return f"⏳ {status}..."

    def format_error(self, error: str) -> List[Dict[str, Any]]:
        """Format an error message."""
        return [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"❌ *Error:* {error}"},
            }
        ]

    def _clean_answer(self, answer: str) -> str:
        """Clean and format answer text for Slack."""
        if not answer:
            return "_No response received_"

        # Convert markdown bold (**text**) to Slack bold (*text*)
        answer = answer.replace("**", "*")

        # Remove verbose explanations that add noise
        skip_phrases = [
            "this count comes from",
            "data spans from",
            "this customer count is derived",
            "suggests we have",
            "based on the data in",
        ]

        lines = answer.split("\n")
        cleaned_lines = [
            line
            for line in lines
            if not any(phrase in line.lower() for phrase in skip_phrases)
        ]
        answer = "\n".join(cleaned_lines)

        # Truncate if too long
        if len(answer) > self.max_response_length:
            answer = answer[: self.max_response_length - 50] + "\n\n_... (truncated)_"

        return answer

    def _text_block(self, text: str) -> Dict[str, Any]:
        """Create a text section block."""
        return {"type": "section", "text": {"type": "mrkdwn", "text": text}}

    def _format_table(self, result: AgentResult) -> List[Dict[str, Any]]:
        """Format result_set as a table block."""
        if not result.result_set:
            return []

        data_rows = result.result_set.get("data", [])
        if not data_rows:
            return []

        num_rows = len(data_rows)
        display_limit = min(self.max_table_rows, num_rows)
        display_rows = data_rows[:display_limit]

        # Build table text
        table_lines = []

        # Header
        if result.column_names:
            header = " | ".join(str(col)[:20] for col in result.column_names)
            table_lines.append(header)
            table_lines.append("-" * min(len(header), 80))

        # Data rows
        for row in display_rows:
            row_text = " | ".join(
                str(val)[:20] if val is not None else "" for val in row
            )
            table_lines.append(row_text)

        table_text = "\n".join(table_lines)

        # Truncate if too long for Slack
        if len(table_text) > 2800:
            table_text = table_text[:2750] + "\n... (truncated)"

        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*📊 Query Results ({num_rows} row{'s' if num_rows != 1 else ''}):*\n```\n{table_text}\n```",
                },
            }
        ]

        # Note about more rows
        if num_rows > display_limit:
            blocks.append(
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"_Showing {display_limit} of {num_rows} rows_",
                        }
                    ],
                }
            )

        return blocks

    def _format_sql(self, sql: str) -> Optional[Dict[str, Any]]:
        """Format SQL as a code block."""
        if not sql or sql == "SQL query executed":
            return None

        # Truncate if too long
        if len(sql) > 2800:
            sql = sql[:2750] + "\n-- ... (truncated)"

        return {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Generated SQL:*\n```sql\n{sql}\n```"},
        }

    def _format_footer(self, result: AgentResult, emoji: str = "🎿") -> Dict[str, Any]:
        """Format metadata footer."""
        tools = ", ".join(set(result.tools_used)) if result.tools_used else "None"

        return {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"{emoji} ⏱️ {result.duration_seconds:.1f}s • 🔧 {tools}",
                }
            ],
        }

    def result_to_csv(self, result: AgentResult) -> Optional[StringIO]:
        """Convert result_set to CSV for file upload."""
        if not result.has_data:
            return None

        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer)

        if result.column_names:
            writer.writerow(result.column_names)

        writer.writerows(result.result_set.get("data", []))
        csv_buffer.seek(0)

        return csv_buffer


class ChartRenderer:
    """
    Render Vega-Lite charts to PNG for Slack upload.

    Requires: altair, vl-convert-python
    """

    def __init__(self, scale: int = 2):
        self.scale = scale
        self._available = self._check_availability()

    def _check_availability(self) -> bool:
        try:
            import vl_convert as vlc

            return True
        except ImportError:
            return False

    @property
    def available(self) -> bool:
        return self._available

    def render(self, chart_spec: Dict) -> Optional[BytesIO]:
        """
        Render Vega-Lite spec to PNG.

        Args:
            chart_spec: Vega-Lite JSON specification

        Returns:
            BytesIO with PNG data, or None if rendering fails
        """
        if not self._available:
            return None

        try:
            import vl_convert as vlc

            png_data = vlc.vegalite_to_png(chart_spec, scale=self.scale)
            return BytesIO(png_data)
        except Exception:
            return None

    def render_all(self, result: AgentResult) -> List[BytesIO]:
        """Render all charts from an AgentResult."""
        if not self._available or not result.chart_specs:
            return []

        rendered = []
        for chart_info in result.chart_specs:
            png = self.render(chart_info.get("spec", {}))
            if png:
                rendered.append(png)

        return rendered
