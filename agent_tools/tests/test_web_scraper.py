"""
Test web scraper functionality - clean Python testing!
"""

import pytest
from unittest.mock import Mock, patch
from src.web_tools.scraper import scrape_website_for_agent, test_locally
from src.web_tools.core import validate_url, extract_prices_from_text


def test_validate_url():
    """Test URL validation"""
    assert validate_url("https://example.com") == True
    assert validate_url("http://example.com") == True
    assert validate_url("not_a_url") == False
    assert validate_url("https://localhost") == False


def test_extract_prices_from_text():
    """Test price extraction"""
    text = (
        "Our plans start at $99/month and go up to $299 per month. Enterprise: $1,500"
    )
    prices = extract_prices_from_text(text)
    assert "$99" in prices
    assert "$299" in prices
    assert "$1,500" in prices


def test_scrape_website_for_agent_invalid_url():
    """Test scraping with invalid URL"""
    mock_session = Mock()
    result = scrape_website_for_agent(mock_session, "invalid_url")

    assert "Web Scraping Failed" in result
    assert "Invalid or blocked URL" in result


@patch("src.web_tools.core.requests")
def test_scrape_website_for_agent_success(mock_requests):
    """Test successful scraping (mocked)"""
    # Mock the requests response
    mock_response = Mock()
    mock_response.content = (
        b"<html><title>Test Company</title><body>Our pricing: $99/month</body></html>"
    )
    mock_response.raise_for_status.return_value = None
    mock_requests.get.return_value = mock_response

    mock_session = Mock()

    # Since we're testing locally, this will use mock data
    result = scrape_website_for_agent(mock_session, "https://example.com")

    assert "Competitive Analysis Report" in result
    assert "example.com" in result


def test_test_locally():
    """Test local testing function"""
    result = test_locally()
    assert len(result) > 100  # Should return substantial HTML
