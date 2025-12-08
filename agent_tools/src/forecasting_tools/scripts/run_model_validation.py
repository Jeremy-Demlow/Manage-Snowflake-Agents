#!/usr/bin/env python
"""
🔍 Model Validation & Drift Detection Script

Purpose: Compare predictions vs actual outcomes, detect model drift, trigger retraining
When to Run: Weekly to validate past week, monthly for comprehensive check
"""
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import warnings

warnings.filterwarnings("ignore")

sys.path.insert(0, ".")

from snowflake.snowpark import Session
import toml
from pathlib import Path


# =============================================================================
# CONNECTION
# =============================================================================
def connect_snowflake():
    """Connect to Snowflake."""
    try:
        from snowflake.snowpark.context import get_active_session

        return get_active_session()
    except:
        config_path = Path.home() / ".snowflake" / "config.toml"
        sf_config = toml.load(config_path)
        conn_config = sf_config.get("connections", {}).get("snowflake_agents", {})

        return Session.builder.configs(
            {
                "account": conn_config.get("account"),
                "user": conn_config.get("user"),
                "password": conn_config.get("password"),
                "database": "SKI_RESORT_DB",
                "schema": "MARTS",
                "warehouse": conn_config.get("warehouse", "COMPUTE_WH"),
            }
        ).create()


def load_validation_data(session):
    """Load predictions joined with actuals."""
    query = """
    WITH predictions AS (
        SELECT
            input_date,
            model_version,
            predicted_visitors,
            prediction_timestamp,
            request_source
        FROM SKI_RESORT_DB.MARTS.ML_PREDICTION_LOG
        WHERE model_name = 'VISITOR_FORECASTER'
          AND input_date <= CURRENT_DATE()
    ),
    actuals AS (
        SELECT
            d.FULL_DATE as actual_date,
            COUNT(DISTINCT p.CUSTOMER_KEY) as actual_visitors
        FROM SKI_RESORT_DB.MARTS.DIM_DATE d
        INNER JOIN SKI_RESORT_DB.MARTS.FACT_PASS_USAGE p ON p.DATE_KEY = d.DATE_KEY
        GROUP BY d.FULL_DATE
    )
    SELECT
        p.input_date,
        p.model_version,
        p.predicted_visitors,
        a.actual_visitors,
        p.predicted_visitors - a.actual_visitors as error,
        ABS(p.predicted_visitors - a.actual_visitors) as abs_error,
        CASE WHEN a.actual_visitors > 0
             THEN ABS(p.predicted_visitors - a.actual_visitors) / a.actual_visitors * 100
             ELSE NULL END as pct_error,
        p.request_source
    FROM predictions p
    JOIN actuals a ON p.input_date = a.actual_date
    ORDER BY p.input_date DESC
    """
    return session.sql(query).to_pandas()


def create_validation_plots(df):
    """Create 4-panel validation visualization."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # 1. Scatter: Predicted vs Actual
    ax1 = axes[0, 0]
    ax1.scatter(
        df["ACTUAL_VISITORS"], df["PREDICTED_VISITORS"], alpha=0.6, c="steelblue"
    )
    max_val = max(df["ACTUAL_VISITORS"].max(), df["PREDICTED_VISITORS"].max())
    ax1.plot([0, max_val], [0, max_val], "r--", label="Perfect prediction")
    ax1.set_xlabel("Actual Visitors")
    ax1.set_ylabel("Predicted Visitors")
    ax1.set_title("Predicted vs Actual Visitors", fontweight="bold")
    ax1.legend()

    # 2. Time series
    ax2 = axes[0, 1]
    df_sorted = df.sort_values("INPUT_DATE")
    ax2.plot(
        df_sorted["INPUT_DATE"],
        df_sorted["ACTUAL_VISITORS"],
        label="Actual",
        marker="o",
        markersize=4,
    )
    ax2.plot(
        df_sorted["INPUT_DATE"],
        df_sorted["PREDICTED_VISITORS"],
        label="Predicted",
        marker="s",
        markersize=4,
    )
    ax2.set_xlabel("Date")
    ax2.set_ylabel("Visitors")
    ax2.set_title("Predictions Over Time", fontweight="bold")
    ax2.legend()
    ax2.tick_params(axis="x", rotation=45)

    # 3. Error distribution
    ax3 = axes[1, 0]
    ax3.hist(df["ERROR"], bins=20, edgecolor="white", color="coral")
    ax3.axvline(x=0, color="red", linestyle="--", label="Zero error")
    ax3.set_xlabel("Prediction Error (Predicted - Actual)")
    ax3.set_ylabel("Frequency")
    ax3.set_title("Error Distribution", fontweight="bold")
    ax3.legend()

    # 4. Error trend (drift detection)
    ax4 = axes[1, 1]
    ax4.plot(
        df_sorted["INPUT_DATE"], df_sorted["PCT_ERROR"], marker="o", color="purple"
    )
    ax4.axhline(y=15, color="orange", linestyle="--", label="15% threshold")
    ax4.axhline(y=25, color="red", linestyle="--", label="25% threshold")
    ax4.fill_between(df_sorted["INPUT_DATE"], 0, 15, alpha=0.2, color="green")
    ax4.set_xlabel("Date")
    ax4.set_ylabel("Percentage Error (%)")
    ax4.set_title("Error Trend (Drift Detection)", fontweight="bold")
    ax4.legend()
    ax4.tick_params(axis="x", rotation=45)

    plt.tight_layout()
    return fig


def main():
    print("=" * 60)
    print("🔍 MODEL VALIDATION & DRIFT DETECTION")
    print("=" * 60)

    session = connect_snowflake()
    print("✅ Connected to Snowflake")

    # Load data
    print("\n📊 Loading validation data...")
    df = load_validation_data(session)

    if len(df) == 0:
        print("⚠️ No validation data found!")
        print("   Run batch predictions first, then wait for actuals.")
        session.close()
        return

    print(f"   Records: {len(df)}")
    print(f"   Date range: {df['INPUT_DATE'].min()} to {df['INPUT_DATE'].max()}")

    # Calculate metrics
    mae = df["ABS_ERROR"].mean()
    mape = df["PCT_ERROR"].dropna().mean()
    rmse = np.sqrt((df["ERROR"] ** 2).mean())

    print("\n📈 OVERALL ACCURACY:")
    print("=" * 50)
    print(f"   MAE:  {mae:.0f} visitors")
    print(f"   MAPE: {mape:.1f}%")
    print(f"   RMSE: {rmse:.0f} visitors")

    # By model version
    print("\n📊 BY MODEL VERSION:")
    version_metrics = (
        df.groupby("MODEL_VERSION")
        .agg({"ABS_ERROR": "mean", "PCT_ERROR": "mean", "INPUT_DATE": "count"})
        .rename(
            columns={"ABS_ERROR": "MAE", "PCT_ERROR": "MAPE", "INPUT_DATE": "Count"}
        )
    )
    print(version_metrics.to_string())

    # Quality gate
    print("\n🎯 QUALITY GATE:")
    if mape < 15:
        print(f"   ✅ MAPE {mape:.1f}% < 15% - Model performing well")
    elif mape < 25:
        print(f"   ⚠️ MAPE {mape:.1f}% - Acceptable but consider retraining")
    else:
        print(f"   🔴 MAPE {mape:.1f}% > 25% - RETRAIN RECOMMENDED!")

    # Drift detection
    if len(df) >= 7:
        recent = df.head(7)["PCT_ERROR"].mean()
        older = df.iloc[7:]["PCT_ERROR"].mean() if len(df) > 7 else recent

        print("\n🔍 DRIFT DETECTION:")
        print(f"   Recent 7-day MAPE: {recent:.1f}%")
        print(f"   Older period MAPE: {older:.1f}%")
        print(f"   Drift: {recent - older:+.1f}%")

        if recent > 25:
            print("\n   🔴 RETRAIN REQUIRED - Recent accuracy degraded")
        elif recent > older * 1.5:
            print("\n   ⚠️ DRIFT DETECTED - Schedule retraining")
        else:
            print("\n   ✅ MODEL HEALTHY - Continue monitoring")

    # Bias check
    avg_error = df["ERROR"].mean()
    print("\n⚖️ BIAS CHECK:")
    if avg_error > 10:
        print(f"   ⚠️ Model OVERPREDICTS by ~{avg_error:.0f} visitors")
    elif avg_error < -10:
        print(f"   ⚠️ Model UNDERPREDICTS by ~{abs(avg_error):.0f} visitors")
    else:
        print(f"   ✅ Minimal bias (avg error: {avg_error:.0f})")

    # Create and save visualization
    if len(df) >= 3:
        fig = create_validation_plots(df)
        fig.savefig("validation_output.png", dpi=150, bbox_inches="tight")
        print("\n📈 Saved visualization: validation_output.png")

    session.close()
    print("\n" + "=" * 60)
    print("✅ VALIDATION COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
