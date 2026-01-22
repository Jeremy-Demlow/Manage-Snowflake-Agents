"""
Local test for the SEND_CONVERSATION_REPORT tool.
Tests PowerPoint generation without Snowflake connection.

Usage:
    python test_conversation_report.py
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from presentation_tools.pptx_generator import create_conversation_pptx


def test_full_report():
    """Test creating a full PowerPoint report with tables and charts."""

    question = "What were our top performing ski resorts by revenue last quarter, and what factors contributed to their success?"

    response = """## Q4 2025 Resort Performance Analysis

### Executive Summary
Our ski resort portfolio showed strong performance in Q4 2025, with total revenue reaching **$45.2M**, representing a 12% YoY increase. Weather conditions and strategic investments in snowmaking drove exceptional results.

### Top 5 Resorts by Revenue

| Resort | Revenue | YoY Growth | Visitor Count | Avg Ticket Price |
|--------|---------|------------|---------------|------------------|
| Alpine Peak | $12,500,000 | +18% | 185,000 | $67.50 |
| Snow Valley | $9,800,000 | +15% | 142,000 | $69.01 |
| Mountain Vista | $8,200,000 | +8% | 125,000 | $65.60 |
| Glacier Run | $7,500,000 | +22% | 98,000 | $76.53 |
| Pine Ridge | $7,200,000 | +5% | 118,000 | $61.02 |

### Key Success Factors

1. **Early Season Snowfall** - Above-average snowfall in November allowed early openings
2. **Snowmaking Investments** - $2.3M invested in new snowmaking equipment at top resorts
3. **Premium Experiences** - New VIP ski packages drove 35% higher per-visitor revenue
4. **Weather Conditions** - Consistent cold temperatures maintained excellent snow quality

### Recommendations

- Expand snowmaking capacity at Mountain Vista and Pine Ridge
- Replicate Glacier Run's premium experience model at other resorts
- Invest in early-season marketing to capitalize on November openings
- Consider dynamic pricing to optimize revenue during peak periods

### Monthly Breakdown

| Month | Revenue | Visitors |
|-------|---------|----------|
| October | $2,100,000 | 28,000 |
| November | $8,500,000 | 125,000 |
| December | $34,600,000 | 515,000 |

*Data sourced from SKI_RESORT_DB.MARTS.FACT_REVENUE*
"""

    print("=" * 60)
    print("Testing SEND_CONVERSATION_REPORT Tool")
    print("=" * 60)
    print(f"\nQuestion: {question[:80]}...")
    print(f"Response length: {len(response)} characters")

    print("\n[1/3] Generating PowerPoint presentation...")
    pptx_path = create_conversation_pptx(
        question=question,
        response=response,
        title="Q4 2025 Resort Performance",
        subtitle="Quarterly Business Review",
    )

    print(f"[2/3] PowerPoint created: {pptx_path}")

    file_size = os.path.getsize(pptx_path)
    print(f"[3/3] File size: {file_size / 1024:.1f} KB")

    print("\n" + "=" * 60)
    print("SUCCESS!")
    print("=" * 60)
    print(f"\nOpen the file to verify:")
    print(f"  open '{pptx_path}'")
    print("\nExpected slides:")
    print("  1. Title slide")
    print("  2. Executive Summary (key insights)")
    print("  3-4. Q&A content slides")
    print("  5-6. Data tables (Top 5 Resorts, Monthly Breakdown)")
    print("  7. Chart (if data extracted successfully)")
    print("  8. Closing slide")

    return pptx_path


def test_minimal_report():
    """Test with minimal content."""
    print("\n" + "-" * 60)
    print("Testing minimal report (no tables)...")

    pptx_path = create_conversation_pptx(
        question="How many visitors did we have yesterday?",
        response="Yesterday we had 2,450 visitors, which is 15% above the daily average.",
        title="Daily Visitor Count",
    )

    print(f"Minimal report created: {pptx_path}")
    return pptx_path


if __name__ == "__main__":
    test_full_report()
    test_minimal_report()
    print("\n✅ All tests passed!")
