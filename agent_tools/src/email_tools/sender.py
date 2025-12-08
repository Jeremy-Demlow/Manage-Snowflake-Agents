"""
Clean email sender implementation - no more SQL string hell!
"""

import logging
from snowflake.snowpark import Session

logger = logging.getLogger(__name__)


def send_email_for_agent(
    session: Session, recipient: str, subject: str, text: str
) -> str:
    """
    Send email using Snowflake's email integration

    This is called directly by the agent via SEND_MAIL procedure
    Clean Python code with proper error handling and logging
    """
    try:
        session.call(
            "SYSTEM$SEND_EMAIL", "ai_email_int", recipient, subject, text, "text/html"
        )
        logger.info(f"Email sent successfully to {recipient} with subject: '{subject}'")
        return f'Email was sent to {recipient} with subject: "{subject}".'

    except Exception as e:
        logger.error(
            f"Failed to send email to {recipient} with subject '{subject}': {e}"
        )
        return f"Failed to send email: {str(e)}"


def test_locally(
    recipient: str = "test@example.com",
    subject: str = "Test Email",
    text: str = "<h1>Test Email</h1><p>This is a test email.</p>",
) -> str:
    """Local testing function - no Snowflake session needed"""
    print(f"[LOCAL TEST] Would send email to: {recipient}")
    print(f"Subject: {subject}")
    print(f"Content: {text[:100]}...")
    return f"Local test: would send email to {recipient}"
