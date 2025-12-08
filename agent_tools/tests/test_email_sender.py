"""
Test email sender functionality - proper Python testing!
"""

import pytest
from unittest.mock import Mock, patch
from src.email_tools.sender import send_email_for_agent, test_locally


def test_send_email_for_agent_success():
    """Test successful email sending"""
    mock_session = Mock()
    mock_session.call.return_value = None

    result = send_email_for_agent(
        mock_session, "test@example.com", "Test Subject", "<h1>Test</h1>"
    )

    assert "Email was sent to test@example.com" in result
    mock_session.call.assert_called_once()


def test_send_email_for_agent_failure():
    """Test email sending failure"""
    mock_session = Mock()
    mock_session.call.side_effect = Exception("Email service unavailable")

    result = send_email_for_agent(
        mock_session, "test@example.com", "Test Subject", "<h1>Test</h1>"
    )

    assert "Failed to send email" in result


def test_test_locally():
    """Test local testing function"""
    result = test_locally()
    assert "Local test: would send email to test@example.com" in result
