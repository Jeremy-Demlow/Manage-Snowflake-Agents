"""
Alerts Processor Procedure Handler
Processes scheduled alerts by calling the agent and sending formatted emails.

Deployed via snowflake.yml as: SKI_RESORT_DB.MODELS.PROCESS_SCHEDULED_ALERTS
"""

from snowflake.snowpark import Session


def process_alerts(session: Session) -> str:
    """
    Process all pending scheduled alerts.

    For each active alert due for sending:
    1. Call ASK_RESORT_EXECUTIVE UDF to get agent response
    2. Call FORMAT_ALERT_EMAIL UDF to convert to styled HTML
    3. Send via SYSTEM$SEND_EMAIL
    4. Log result and update last_sent timestamp

    Args:
        session: Snowpark session

    Returns:
        Summary of processed alerts
    """
    # Get pending alerts
    alerts_df = session.sql(
        """
        SELECT alert_id, alert_name, recipient_email, question
        FROM SKI_RESORT_DB.MODELS.SCHEDULED_ALERTS
        WHERE is_active = TRUE
          AND frequency = 'daily'
          AND (last_sent_at IS NULL OR last_sent_at < CURRENT_DATE())
    """
    ).collect()

    processed = 0
    failed = 0

    for row in alerts_df:
        alert_id = row["ALERT_ID"]
        alert_name = row["ALERT_NAME"]
        recipient = row["RECIPIENT_EMAIL"]
        question = row["QUESTION"]

        try:
            # Call agent via UDF
            response_df = session.sql(
                f"""
                SELECT SKI_RESORT_DB.MODELS.ASK_RESORT_EXECUTIVE('{_escape_sql(question)}') as response
            """
            ).collect()
            response = (
                response_df[0]["RESPONSE"] if response_df else "No response from agent"
            )

            # Check for error
            if response.startswith("API Error") or response.startswith("Error"):
                raise Exception(response)

            # Format email using formatter UDF
            html_df = session.sql(
                f"""
                SELECT SKI_RESORT_DB.MODELS.FORMAT_ALERT_EMAIL(
                    '{_escape_sql(question)}',
                    '{_escape_sql(response)}'
                ) as html
            """
            ).collect()
            html_body = html_df[0]["HTML"] if html_df else f"<pre>{response}</pre>"

            subject = f"Ski Resort Alert: {alert_name}"

            # Send email
            session.call(
                "SYSTEM$SEND_EMAIL",
                "ai_email_int",
                recipient,
                subject,
                html_body,
                "text/html",
            )

            # Log success
            session.sql(
                f"""
                INSERT INTO SKI_RESORT_DB.MODELS.ALERT_LOG (alert_id, question, response, status)
                VALUES ('{alert_id}', '{_escape_sql(question)}', '{_escape_sql(response[:4000])}', 'success')
            """
            ).collect()

            # Update last_sent
            session.sql(
                f"""
                UPDATE SKI_RESORT_DB.MODELS.SCHEDULED_ALERTS
                SET last_sent_at = CURRENT_TIMESTAMP()
                WHERE alert_id = '{alert_id}'
            """
            ).collect()

            processed += 1

        except Exception as e:
            # Log failure
            session.sql(
                f"""
                INSERT INTO SKI_RESORT_DB.MODELS.ALERT_LOG (alert_id, question, status, error_message)
                VALUES ('{alert_id}', '{_escape_sql(question)}', 'failed', '{_escape_sql(str(e)[:500])}')
            """
            ).collect()
            failed += 1

    return f"Processed: {processed} alerts, Failed: {failed}"


def _escape_sql(text: str) -> str:
    """Escape single quotes for SQL strings."""
    if text is None:
        return ""
    return text.replace("'", "''")
