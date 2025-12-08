"""
Scheduled Alert Service

Orchestrates the complete alert workflow:
1. Fetch active alerts from database
2. Call Cortex Agent for each alert
3. Format response as HTML email
4. Send email via Snowflake

Supports multiple runtime environments:
- Local development
- SPCS/Container Runtime (when agent API auth is supported)
- External schedulers (Airflow, etc.)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from scheduled_alerts.agent_client import AgentClient
from scheduled_alerts.email_formatter import EmailFormatter, EmailConfig

logger = logging.getLogger(__name__)


@dataclass
class AlertConfig:
    """Configuration for alert service."""

    database: str = "SKI_RESORT_DB"
    schema: str = "AGENTS"
    agent_name: str = "RESORT_EXECUTIVE_DEV"
    alerts_table: str = "SCHEDULED_ALERTS"
    email_integration: str = "MY_EMAIL_INT"

    @property
    def full_table_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.alerts_table}"


@dataclass
class Alert:
    """Represents a scheduled alert."""

    alert_id: int
    user_email: str
    question: str
    frequency: str
    agent_name: str = ""
    is_active: bool = True
    created_at: Optional[datetime] = None
    last_sent_at: Optional[datetime] = None
    send_count: int = 0


@dataclass
class AlertResult:
    """Result of processing an alert."""

    alert_id: int
    user_email: str
    status: str  # "success", "error", "skipped"
    error: Optional[str] = None
    response_length: int = 0
    duration_ms: int = 0


class AlertService:
    """
    Service for processing scheduled alerts.

    Example usage:

        from snowflake.snowpark import Session
        from scheduled_alerts.alert_service import AlertService

        session = Session.builder.config("connection_name", "my_conn").create()
        service = AlertService.from_session(session)
        results = service.process_all_alerts()
    """

    def __init__(
        self,
        session,
        agent_client: AgentClient,
        config: Optional[AlertConfig] = None,
        email_config: Optional[EmailConfig] = None,
    ):
        self._session = session
        self._agent = agent_client
        self.config = config or AlertConfig()
        self._formatter = EmailFormatter(email_config)

    @classmethod
    def from_session(
        cls,
        session,
        config: Optional[AlertConfig] = None,
        email_config: Optional[EmailConfig] = None,
    ) -> "AlertService":
        """Create alert service from Snowpark session."""
        cfg = config or AlertConfig()
        agent_client = AgentClient.from_session(
            session, agent_name=cfg.agent_name, database=cfg.database, schema=cfg.schema
        )
        return cls(session, agent_client, cfg, email_config)

    @classmethod
    def from_container(
        cls,
        session,
        config: Optional[AlertConfig] = None,
        email_config: Optional[EmailConfig] = None,
    ) -> "AlertService":
        """
        Create alert service for SPCS/Container Runtime.

        Note: Agent API auth from containers is a known gap as of Dec 2024.
        """
        cfg = config or AlertConfig()
        agent_client = AgentClient.from_container(
            agent_name=cfg.agent_name, database=cfg.database, schema=cfg.schema
        )
        return cls(session, agent_client, cfg, email_config)

    def _fetch_active_alerts(self) -> List[Alert]:
        """Fetch all active alerts from the database."""
        sql = f"""
            SELECT
                ALERT_ID,
                USER_EMAIL,
                OVERALL_QUESTION,
                ALERT_FREQUENCY,
                COALESCE(AGENT_NAME, '{self.config.agent_name}') as AGENT_NAME,
                IS_ACTIVE,
                CREATED_AT,
                LAST_SENT_AT,
                SEND_COUNT
            FROM {self.config.full_table_name}
            WHERE IS_ACTIVE = TRUE
            ORDER BY CREATED_AT
        """

        rows = self._session.sql(sql).collect()

        alerts = []
        for row in rows:
            alerts.append(
                Alert(
                    alert_id=row["ALERT_ID"],
                    user_email=row["USER_EMAIL"],
                    question=row["OVERALL_QUESTION"],
                    frequency=row["ALERT_FREQUENCY"],
                    agent_name=row["AGENT_NAME"],
                    is_active=row["IS_ACTIVE"],
                    created_at=row["CREATED_AT"],
                    last_sent_at=row["LAST_SENT_AT"],
                    send_count=row["SEND_COUNT"],
                )
            )

        return alerts

    def _send_email(self, recipient: str, subject: str, html: str) -> None:
        """Send email via Snowflake SYSTEM$SEND_EMAIL."""
        escaped_html = html.replace("'", "''")
        escaped_subject = subject.replace("'", "''")

        sql = f"""
            CALL SYSTEM$SEND_EMAIL(
                '{self.config.email_integration}',
                '{recipient}',
                '{escaped_subject}',
                '{escaped_html}',
                'text/html'
            )
        """

        self._session.sql(sql).collect()
        logger.info(f"Email sent to {recipient}")

    def _update_alert_sent(self, alert_id: int) -> None:
        """Update alert's last_sent_at and send_count."""
        sql = f"""
            UPDATE {self.config.full_table_name}
            SET
                LAST_SENT_AT = CURRENT_TIMESTAMP(),
                SEND_COUNT = SEND_COUNT + 1
            WHERE ALERT_ID = {alert_id}
        """
        self._session.sql(sql).collect()

    def _process_single_alert(self, alert: Alert) -> AlertResult:
        """Process a single alert."""
        start_time = datetime.now()

        logger.info(f"Processing alert {alert.alert_id} for {alert.user_email}")

        response = self._agent.ask_detailed(alert.question)

        if not response.has_content:
            return AlertResult(
                alert_id=alert.alert_id,
                user_email=alert.user_email,
                status="skipped",
                error="Empty or insufficient response from agent",
            )

        html = self._formatter.format_with_inline_css(
            question=alert.question, response=response.text
        )

        question_preview = (
            alert.question[:50] + "..." if len(alert.question) > 50 else alert.question
        )
        subject = f"🎿 {question_preview}"

        self._send_email(alert.user_email, subject, html)
        self._update_alert_sent(alert.alert_id)

        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)

        return AlertResult(
            alert_id=alert.alert_id,
            user_email=alert.user_email,
            status="success",
            response_length=len(response.text),
            duration_ms=duration_ms,
        )

    def process_all_alerts(self) -> List[AlertResult]:
        """Process all active alerts."""
        alerts = self._fetch_active_alerts()
        logger.info(f"Found {len(alerts)} active alerts")

        results = []
        for alert in alerts:
            try:
                result = self._process_single_alert(alert)
            except Exception as e:
                logger.error(f"Error processing alert {alert.alert_id}: {e}")
                result = AlertResult(
                    alert_id=alert.alert_id,
                    user_email=alert.user_email,
                    status="error",
                    error=str(e),
                )
            results.append(result)

            if result.status == "success":
                logger.info(f"Alert {alert.alert_id}: sent to {alert.user_email}")
            elif result.status == "error":
                logger.error(f"Alert {alert.alert_id}: {result.error}")

        success_count = sum(1 for r in results if r.status == "success")
        error_count = sum(1 for r in results if r.status == "error")
        logger.info(
            f"Completed: {success_count} sent, {error_count} errors, {len(results)} total"
        )

        return results

    def send_test_alert(self, email: str, question: str) -> AlertResult:
        """Send a one-time test alert."""
        start_time = datetime.now()

        logger.info(f"Sending test alert to {email}")

        response = self._agent.ask_detailed(question)

        if not response.has_content:
            return AlertResult(
                alert_id=0,
                user_email=email,
                status="skipped",
                error="Empty or insufficient response from agent",
            )

        html = self._formatter.format_with_inline_css(
            question=question, response=response.text
        )

        question_preview = question[:50] + "..." if len(question) > 50 else question
        subject = f"🎿 [TEST] {question_preview}"

        self._send_email(email, subject, html)

        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)

        return AlertResult(
            alert_id=0,
            user_email=email,
            status="success",
            response_length=len(response.text),
            duration_ms=duration_ms,
        )

    def to_json(self, results: List[AlertResult]) -> str:
        """Convert results to JSON string."""
        return json.dumps(
            [
                {
                    "alert_id": r.alert_id,
                    "user_email": r.user_email,
                    "status": r.status,
                    "error": r.error,
                    "response_length": r.response_length,
                    "duration_ms": r.duration_ms,
                }
                for r in results
            ],
            indent=2,
        )
