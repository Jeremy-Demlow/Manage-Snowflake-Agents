"""
Core web scraping functionality - clean Python, no string escaping!
"""

import logging
import time
import re
from urllib.parse import urlparse
from typing import Dict, Any, List

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    requests = None
    BeautifulSoup = None

logger = logging.getLogger(__name__)


def validate_url(url: str) -> bool:
    """Validate URL format and security"""
    if not url or not isinstance(url, str):
        return False

    if not (url.startswith("http://") or url.startswith("https://")):
        return False

    parsed = urlparse(url)
    if not parsed.netloc:
        return False

    # Security: Block internal networks
    if parsed.hostname in ["localhost", "127.0.0.1"] or (
        parsed.hostname and parsed.hostname.startswith("192.168.")
    ):
        logger.warning(f"Blocked internal URL: {url}")
        return False

    return True


def scrape_single_url(url: str) -> Dict[str, Any]:
    """
    Scrape a single URL and return structured data
    Clean Python function - no SQL strings!
    """
    if not validate_url(url):
        return {
            "success": False,
            "url": url,
            "error": "Invalid or blocked URL",
            "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    if not requests or not BeautifulSoup:
        # Error if dependencies not available - no more mock data
        return {
            "success": False,
            "url": url,
            "error": "Required dependencies (requests, beautifulsoup4) not available",
            "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; SnowflakeAgent/1.0)"}

        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")

        # Remove noise elements
        for element in soup(["script", "style", "nav", "footer"]):
            element.decompose()

        title = soup.find("title")
        title_text = title.get_text().strip() if title else urlparse(url).netloc

        # Get main content
        main = soup.find("main") or soup.find("article") or soup.body
        content_text = (
            main.get_text(separator=" ", strip=True)
            if main
            else soup.get_text(separator=" ", strip=True)
        )
        content_text = re.sub(r"\s+", " ", content_text).strip()

        # Extract prices for competitive analysis
        prices = extract_prices_from_text(content_text)

        return {
            "success": True,
            "url": url,
            "title": title_text,
            "content": content_text[
                :5000
            ],  # Increased content length for better analysis
            "prices": prices,
            "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    except Exception as e:
        logger.error(f"Scraping failed for {url}: {e}")
        return {
            "success": False,
            "url": url,
            "error": str(e),
            "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }


def extract_prices_from_text(text: str) -> List[str]:
    """Extract price patterns from text for competitive analysis"""
    price_patterns = [
        r"\$[\d,]+\.?\d*",
        r"€[\d,]+\.?\d*",
        r"£[\d,]+\.?\d*",
        r"USD\s*[\d,]+\.?\d*",
        r"EUR\s*[\d,]+\.?\d*",
    ]

    prices = []
    for pattern in price_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        prices.extend(matches[:3])  # Limit per pattern

    return list(set(prices))[:5]  # Remove duplicates, limit total


def create_mock_scrape_result(url: str) -> Dict[str, Any]:
    """Create realistic mock data for local testing"""
    domain = urlparse(url).netloc

    content = f"""
    Welcome to {domain} - Industry leader in business solutions.

    Our pricing tiers:
    - Starter Plan: $99/month
    - Professional: $299/month
    - Enterprise: $599/month

    Key features:
    - Advanced analytics dashboard
    - 24/7 customer support
    - API integrations
    - Custom reporting
    - Real-time data sync

    Contact our sales team for competitive analysis and market insights.
    Over 10,000+ customers trust our platform for their business intelligence needs.
    """

    return {
        "success": True,
        "url": url,
        "title": f"Business Solutions - {domain}",
        "content": content,
        "prices": extract_prices_from_text(content),
        "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def get_competitor_suggestions(primary_url: str) -> List[str]:
    """Mock competitor suggestions for testing"""
    domain = urlparse(primary_url).netloc.replace("www.", "")
    base = domain.split(".")[0]

    return [
        f"https://competitor1-{base}.com",
        f"https://alternative-{base}.com",
        f"https://{base}-rival.com",
    ]
