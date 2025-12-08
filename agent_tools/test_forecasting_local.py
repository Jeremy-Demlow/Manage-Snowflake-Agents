"""
Local test for forecasting tools
Tests Python functions before deploying to Snowflake
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "data_generation"))
sys.path.insert(0, str(Path(__file__).parent / "src"))

from snowflake_connection import SnowflakeConnection
from forecasting_tools import predict_daily_visitors, get_peak_days_analysis
from forecasting_tools.staffing_optimizer import optimize_lift_staffing
import json


def test_visitor_forecast():
    """Test visitor forecasting function"""
    print("=" * 80)
    print("TEST 1: VISITOR FORECASTING")
    print("=" * 80)

    conn = SnowflakeConnection.from_snow_cli("agents_example")

    # Forecast next 7 days
    result = predict_daily_visitors(conn.session, "2024-12-20", 7)

    forecast = json.loads(result)
    print(f"\nForecast Start: {forecast['forecast_start_date']}")
    print(f"Days Forecasted: {forecast['days_forecasted']}")
    print(
        f"Historical Avg: {forecast['historical_context']['avg_visitors_all_time']} visitors/day"
    )
    print(f"\nNext 7 Days:")
    for day in forecast["forecasts"]:
        print(
            f"  {day['date']} ({day['day_of_week']}): {day['predicted_visitors']} visitors - {day['staffing']['level']}"
        )

    conn.close()
    print("\n✓ Test passed!\n")


def test_peak_days():
    """Test peak days analysis"""
    print("=" * 80)
    print("TEST 2: PEAK DAYS ANALYSIS")
    print("=" * 80)

    conn = SnowflakeConnection.from_snow_cli("agents_example")

    result = get_peak_days_analysis(conn.session, "2023-2024")

    analysis = json.loads(result)
    print(f"\nSeason: {analysis['season']}")
    print(f"Max Visitors: {analysis['statistics']['max_visitors_observed']}")
    print(f"Staffing Rec: {analysis['statistics']['staffing_recommendation']}")
    print(f"\nTop 5 Busiest Days:")
    for day in analysis["top_peak_days"][:5]:
        print(
            f"  {day['date']} ({day['day_name']}): {day['visitors']} visitors - Holiday: {day['is_holiday']}"
        )

    conn.close()
    print("\n✓ Test passed!\n")


def test_lift_staffing():
    """Test lift staffing optimization"""
    print("=" * 80)
    print("TEST 3: LIFT STAFFING OPTIMIZATION")
    print("=" * 80)

    conn = SnowflakeConnection.from_snow_cli("agents_example")

    result = optimize_lift_staffing(
        conn.session, "2024-01-15", 15  # 15 minute threshold
    )

    optimization = json.loads(result)
    print(f"\nTarget Date: {optimization['target_date']}")
    print(f"Max Wait Threshold: {optimization['max_wait_threshold_minutes']} minutes")
    print(
        f"Lifts Needing Staff Increase: {optimization['summary']['lifts_needing_increase']}"
    )
    print(f"\nTop 5 Lifts by Priority:")

    sorted_lifts = sorted(
        optimization["lift_recommendations"],
        key=lambda x: (
            x["recommendation"]["priority"] == "HIGH",
            x["metrics"]["avg_wait_minutes"],
        ),
        reverse=True,
    )

    for lift in sorted_lifts[:5]:
        print(
            f"  {lift['lift_name']} ({lift['terrain']}): {lift['metrics']['avg_wait_minutes']}min avg wait - {lift['recommendation']['action']}"
        )

    conn.close()
    print("\n✓ Test passed!\n")


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("SKI RESORT FORECASTING TOOLS - LOCAL TESTING")
    print("=" * 80 + "\n")

    try:
        test_visitor_forecast()
        test_peak_days()
        test_lift_staffing()

        print("=" * 80)
        print("✓✓✓ ALL TESTS PASSED")
        print("=" * 80)
        print("\nForecasting tools are working locally!")
        print("Deploy to Snowflake with: python src/forecasting_tools/deploy.py")
        print("Or use existing SQL procedures (already deployed)")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
