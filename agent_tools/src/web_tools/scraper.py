"""
Main web scraper entry point for agents - clean Python, no SQL strings!
"""

import logging
from typing import List

try:
    from snowflake.snowpark import Session
except ImportError:
    Session = None
from .core import scrape_single_url, validate_url
from .analyzer import (
    get_competitor_suggestions,
    analyze_competitive_landscape,
    create_html_report,
)

logger = logging.getLogger(__name__)


def scrape_website_for_agent(session: Session, url: str) -> str:
    """
    Main entry point for Snowflake Intelligence agents

    This is called via the WEB_SCRAPE procedure
    Returns beautiful HTML competitive analysis report

    Clean Python code - no more string escaping nightmare!
    """
    try:
        # Validate URL
        if not validate_url(url):
            return create_error_html(url, "Invalid or blocked URL")

        logger.info(f"Starting competitive analysis for: {url}")

        # Step 1: Scrape primary URL
        primary_result = scrape_single_url(url)
        if not primary_result["success"]:
            return create_error_html(
                url, primary_result.get("error", "Scraping failed")
            )

        # Step 2: Get AI competitor suggestions
        competitor_urls = get_competitor_suggestions(
            session, url, primary_result["content"]
        )

        # Step 3: Scrape competitors (limited to 3 for performance)
        competitor_results = []
        for competitor_url in competitor_urls[:3]:
            logger.info(f"Scraping competitor: {competitor_url}")
            result = scrape_single_url(competitor_url)
            competitor_results.append(result)

        # Step 4: AI analysis of competitive landscape
        logger.info("Performing AI competitive analysis")
        analysis = analyze_competitive_landscape(
            session, primary_result, competitor_results
        )

        # Step 5: Create beautiful HTML report for agent
        return create_html_report(primary_result, competitor_results, analysis)

    except Exception as e:
        logger.error(f"Competitive analysis failed: {e}")
        return create_error_html(url, str(e))


def create_error_html(url: str, error: str) -> str:
    """Create error response in HTML format"""
    return f"""<div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background-color: #ffebee; padding: 20px; border-radius: 5px; border-left: 4px solid #f44336;">
            <h3 style="color: #c62828; margin-top: 0;">❌ Web Scraping Failed</h3>
            <p><strong>URL:</strong> {url}</p>
            <p><strong>Error:</strong> {error}</p>
            <p style="margin-bottom: 0;"><em>Please verify the URL and try again.</em></p>
        </div>
    </div>"""


def test_locally(url: str = "https://snowflake.com") -> str:
    """Local testing function - no Snowflake session needed"""
    print(f"[LOCAL TEST] Would analyze: {url}")

    # Mock session for testing
    result = scrape_website_for_agent(None, url)
    print(f"HTML result length: {len(result)} characters")
    print("HTML preview:", result[:200] + "..." if len(result) > 200 else result)
    return result
