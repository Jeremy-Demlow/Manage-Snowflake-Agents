#!/usr/bin/env python3
"""
Demo Prep Script: Generate fresh data and refresh DBT models.

This script ensures the ski resort demo has up-to-date data by:
1. Generating incremental data for recent days
2. Running dbt to refresh all fact tables
3. Optionally retraining the ML model

Usage:
    python scripts/demo_prep.py                    # Generate last 30 days
    python scripts/demo_prep.py --days 60         # Generate last 60 days
    python scripts/demo_prep.py --retrain         # Also retrain ML model
"""

import argparse
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path


def run_command(cmd: list[str], cwd: str = None, description: str = "") -> bool:
    """Run a shell command and return success status."""
    print(f"\n{'='*60}")
    print(f"📌 {description}")
    print(f"{'='*60}")
    print(f"$ {' '.join(cmd)}")

    result = subprocess.run(cmd, cwd=cwd)

    if result.returncode != 0:
        print(f"❌ FAILED: {description}")
        return False

    print(f"✅ SUCCESS: {description}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Prepare demo data")
    parser.add_argument('--days', type=int, default=30,
                        help='Number of days to generate (default: 30)')
    parser.add_argument('--connection', type=str, default='snowflake_agents',
                        help='Snowflake CLI connection name')
    parser.add_argument('--retrain', action='store_true',
                        help='Retrain the ML model after data refresh')
    parser.add_argument('--start-date', type=str, default=None,
                        help='Start date (YYYY-MM-DD). Defaults to <days> ago.')
    args = parser.parse_args()

    # Calculate start date
    if args.start_date:
        start_date = args.start_date
    else:
        # Default to ski season dates if we're in ski season
        today = datetime.now()
        # Generate from start of current ski season (Nov 1)
        if today.month >= 11:
            start_date = f"{today.year}-11-01"
        elif today.month <= 4:
            start_date = f"{today.year - 1}-11-01"
        else:
            # Off-season - generate from last April
            start_date = (today - timedelta(days=args.days)).strftime('%Y-%m-%d')

    # Calculate days to cover if using ski season logic
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    days_to_generate = (datetime.now() - start_dt).days + 1
    if args.start_date is None:
        args.days = max(args.days, days_to_generate)

    project_root = Path(__file__).parent.parent.absolute()

    print("\n" + "🎿" * 30)
    print("   SKI RESORT DEMO PREP")
    print("🎿" * 30)
    print(f"\n📅 Generating data from {start_date} ({args.days} days)")
    print(f"🔗 Connection: {args.connection}")
    print(f"📁 Project root: {project_root}")

    # Step 1: Generate incremental data
    success = run_command(
        [
            "python", "generate_daily_increment.py",
            "--connection", args.connection,
            "--date", start_date,
            "--days", str(args.days)
        ],
        cwd=str(project_root / "data_generation"),
        description="Generating incremental data"
    )

    if not success:
        print("\n💥 Data generation failed. Aborting.")
        sys.exit(1)

    # Step 2: Run dbt refresh
    success = run_command(
        [
            "dbt", "run", "--full-refresh",
            "--select",
            "fact_weather", "fact_pass_usage", "fact_lift_scans",
            "fact_food_beverage", "fact_rentals", "fact_ticket_sales"
        ],
        cwd=str(project_root / "dbt_ski_resort"),
        description="Refreshing DBT fact tables"
    )

    if not success:
        print("\n💥 DBT refresh failed. Aborting.")
        sys.exit(1)

    # Step 3: Optionally retrain model
    if args.retrain:
        print("\n⚠️  ML model retraining requested.")
        print("   Please run the training notebook manually:")
        print(f"   {project_root / 'agent_tools/src/forecasting_tools/notebooks/train_visitor_forecast.ipynb'}")

    # Summary
    print("\n" + "="*60)
    print("🎉 DEMO PREP COMPLETE!")
    print("="*60)
    print(f"""
Next steps:
1. Run the training notebook to update the ML model:
   jupyter notebook agent_tools/src/forecasting_tools/notebooks/train_visitor_forecast.ipynb

2. Verify data in Snowflake:
   SELECT MIN(WEATHER_DATE), MAX(WEATHER_DATE) FROM SKI_RESORT_DB.MARTS.FACT_WEATHER;

3. Test the model prediction:
   python agent_tools/src/forecasting_tools/test_predict_batch.py
""")


if __name__ == "__main__":
    main()
