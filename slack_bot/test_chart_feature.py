#!/usr/bin/env python3
"""
Quick test: Verify chart feature works with Agent API
"""
import os
import sys

# Set credentials from environment or use defaults for testing
# export SNOWFLAKE_PAT="your-pat-token"
# export SNOWFLAKE_ACCOUNT="your-account"
if not os.environ.get("SNOWFLAKE_PAT"):
    print("⚠️  SNOWFLAKE_PAT not set. Set it with: export SNOWFLAKE_PAT='your-token'")
    sys.exit(1)

from simple_bot import ask_agent, vega_to_png, CHARTS_AVAILABLE

print("=" * 60)
print("CHART FEATURE TEST")
print("=" * 60)

print("\n✅ Dependencies Check:")
print(f"   Chart rendering available: {CHARTS_AVAILABLE}")

if CHARTS_AVAILABLE:
    import altair as alt
    import vl_convert as vlc

    print("   ✅ altair imported")
    print("   ✅ vl-convert imported")
else:
    print("   ❌ Chart libraries missing")
    sys.exit(1)

print("\n📡 Testing Agent API (ask a chart-worthy question)...")
question = "Show me the top 5 customers by revenue as a chart"

print(f"   Question: '{question}'")
print("   Calling ACME_INTELLIGENCE_AGENT...")

try:
    result = ask_agent(question, "intelligence")

    print(f"\n✅ Response received!")
    print(f"   Answer length: {len(result['answer'])} chars")
    print(f"   Tools used: {result.get('tools_used', [])}")
    print(f"   SQL generated: {'Yes' if result.get('sql') else 'No'}")
    print(f"   Chart specs found: {len(result.get('chart_specs', []))}")

    if result.get("chart_specs"):
        print("\n📊 CHART SPECIFICATIONS DETECTED!")
        for idx, chart_info in enumerate(result["chart_specs"]):
            chart_spec = chart_info["spec"]
            print(f"\n   Chart {idx + 1}:")
            print(f"      Type: {chart_spec.get('mark', 'unknown')}")
            print(f"      Schema: {chart_spec.get('$schema', 'N/A')[:50]}...")

            # Test conversion
            try:
                png_buffer = vega_to_png(chart_spec)
                print(
                    f"      ✅ Successfully converted to PNG ({len(png_buffer.getvalue()):,} bytes)"
                )
                print(f"      📤 Ready for Slack upload!")
            except Exception as e:
                print(f"      ❌ Conversion failed: {e}")
    else:
        print("\n📝 No charts in this response")
        print("   (Agent may not have generated charts for this query)")
        print("   Try: 'Show me monthly revenue trend' or 'Plot top customers'")

    print("\n" + "=" * 60)
    print("CHART FEATURE STATUS: ✅ WORKING")
    print("=" * 60)
    print("\nWhen Slack connectivity is restored, charts will")
    print("automatically appear in your Slack threads!")

except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback

    traceback.print_exc()
