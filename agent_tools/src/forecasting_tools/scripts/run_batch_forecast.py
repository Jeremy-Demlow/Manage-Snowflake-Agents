#!/usr/bin/env python
"""
📊 Batch Visitor Forecast Script

Purpose: Generate detailed visitor forecasts with visualizations for planning
When to Run: Weekly for next week's planning, before holidays, ad-hoc for events
"""
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import json
import yaml
import warnings

warnings.filterwarnings("ignore")

# Add models to path
sys.path.insert(0, ".")

from models.feature_engineer import FeatureEngineer
from snowflake.ml.registry import Registry
from snowflake.snowpark import Session
import toml
from pathlib import Path

# =============================================================================
# CONFIGURATION - Load from YAML (single source of truth)
# =============================================================================
CONFIG_PATH = Path(__file__).parent.parent / "config" / "model_config.yaml"
FORECAST_DAYS = 14  # Two weeks ahead


def load_config():
    """Load model configuration from YAML."""
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


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


def get_weather_defaults(session, target_month):
    """Get historical weather averages for target month."""
    query = f"""
    SELECT
        AVG(w.SNOWFALL_INCHES) as avg_snowfall,
        AVG(w.BASE_DEPTH_INCHES) as avg_base_depth,
        AVG(w.AVG_TEMP_F) as avg_temp,
        AVG(w.WIND_SPEED_MPH) as avg_wind,
        AVG(CASE WHEN w.IS_POWDER_DAY THEN 1 ELSE 0 END) as powder_rate
    FROM SKI_RESORT_DB.MARTS.FACT_WEATHER w
    JOIN SKI_RESORT_DB.MARTS.DIM_DATE d ON w.DATE_KEY = d.DATE_KEY
    WHERE d.MONTH_NUM = {target_month}
    """
    avgs = session.sql(query).to_pandas().iloc[0]

    return {
        "snowfall_inches": float(avgs["AVG_SNOWFALL"]),
        "base_depth_inches": float(avgs["AVG_BASE_DEPTH"]),
        "avg_temp_f": float(avgs["AVG_TEMP"]),
        "wind_speed_mph": float(avgs["AVG_WIND"]),
        "is_powder_day": 1 if avgs["POWDER_RATE"] > 0.3 else 0,
        "is_high_wind": 0,
    }


def get_staffing(visitors, config=None):
    """Get staffing recommendations based on visitor count from YAML config."""
    if config is None:
        config = load_config()

    staffing_levels = config.get("staffing_levels", {})

    # Find the appropriate level based on max_visitors threshold
    for level_name in ["minimal", "light", "moderate", "high", "maximum"]:
        level = staffing_levels.get(level_name, {})
        if visitors < level.get("max_visitors", 999999):
            return (
                level_name.title(),
                level.get("lift_operators", "TBD"),
                level.get("f_and_b", "TBD"),
            )

    # Default fallback
    return "Maximum", "22+", "All + overflow"


def create_visualization(results, forecast_start, forecast_days):
    """Create forecast bar chart."""
    fig, ax = plt.subplots(figsize=(14, 6))

    colors = {
        "Minimal": "#2ecc71",
        "Light": "#3498db",
        "Moderate": "#f39c12",
        "High": "#e74c3c",
        "Maximum": "#9b59b6",
    }

    bar_colors = [colors[level] for level in results["staffing_level"]]
    bars = ax.bar(
        range(len(results)),
        results["predicted_visitors"],
        color=bar_colors,
        edgecolor="white",
        linewidth=0.5,
    )

    ax.set_xticks(range(len(results)))
    ax.set_xticklabels(
        [f"{d.strftime('%a')}\n{d.strftime('%m/%d')}" for d in results["date"]],
        fontsize=9,
    )
    ax.set_ylabel("Predicted Visitors", fontsize=12)
    ax.set_title(
        f"🎿 Visitor Forecast: {forecast_start} to {forecast_start + timedelta(days=forecast_days-1)}",
        fontsize=14,
        fontweight="bold",
    )

    for bar, val in zip(bars, results["predicted_visitors"]):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 10,
            f"{val:,}",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    from matplotlib.patches import Patch

    legend_elements = [Patch(facecolor=c, label=l) for l, c in colors.items()]
    ax.legend(handles=legend_elements, title="Staffing Level", loc="upper right")

    plt.tight_layout()
    return fig


def main():
    print("=" * 60)
    print("📊 BATCH VISITOR FORECAST")
    print("=" * 60)

    # Connect
    session = connect_snowflake()
    print("✅ Connected to Snowflake")

    # Configuration
    forecast_start = datetime.now().date() + timedelta(days=1)
    forecast_dates = [forecast_start + timedelta(days=i) for i in range(FORECAST_DAYS)]

    print(
        f"\n📅 Forecast: {forecast_start} to {forecast_start + timedelta(days=FORECAST_DAYS-1)}"
    )

    # Load model
    registry = Registry(session, database_name="SKI_RESORT_DB", schema_name="MODELS")
    model = registry.get_model("VISITOR_FORECASTER")
    model_version = model.default
    print(f"🤖 Model: VISITOR_FORECASTER v{model_version.version_name}")

    # Load feature engineer
    fe = FeatureEngineer.from_config("visitor_forecast")

    # Get historical data
    query = fe.get_training_query(
        database="SKI_RESORT_DB", schema="MARTS", years_back=1
    )
    raw_df = session.sql(query).to_pandas()
    hist_df = fe.engineer(raw_df)
    print(
        f"📈 Historical: {len(hist_df)} records, recent avg: {hist_df['UNIQUE_VISITORS'].tail(7).mean():.0f}"
    )

    # Weather defaults
    weather_defaults = get_weather_defaults(session, forecast_start.month)
    print(
        f"🌨️ Weather defaults (month {forecast_start.month}): base={weather_defaults['base_depth_inches']:.0f}in"
    )

    # Build features for prediction
    feature_cols = fe.get_feature_columns(hist_df)

    # Get recent values for lag features
    recent_values = {
        "lag_7": float(hist_df["UNIQUE_VISITORS"].tail(7).mean()),
        "lag_14": float(hist_df["UNIQUE_VISITORS"].tail(14).mean()),
        "rolling_7_mean": float(hist_df["UNIQUE_VISITORS"].tail(7).mean()),
        "rolling_30_mean": float(hist_df["UNIQUE_VISITORS"].tail(30).mean()),
    }

    future_df = fe.build_future_features(
        start_date=forecast_start,
        days_ahead=FORECAST_DAYS,
        recent_values=recent_values,
        weather_forecast=weather_defaults,
    )
    input_df = future_df[feature_cols].astype(float)

    # Predict
    print("\n🔮 Running predictions...")
    input_sp = session.create_dataframe(input_df)
    predictions_sp = model_version.run(input_sp, function_name="predict")
    predictions_df = predictions_sp.to_pandas()

    pred_col = [c for c in predictions_df.columns if "predict" in c.lower()][0]
    predicted_values = predictions_df[pred_col].values

    # Build results
    results = pd.DataFrame(
        {
            "date": forecast_dates,
            "day_name": [d.strftime("%A") for d in forecast_dates],
            "predicted_visitors": [int(max(0, v)) for v in predicted_values],
        }
    )

    results["staffing_level"], results["lift_operators"], results["f_and_b"] = zip(
        *results["predicted_visitors"].apply(get_staffing)
    )

    # Print results
    print("\n📋 FORECAST RESULTS:")
    print("=" * 50)
    for _, row in results.iterrows():
        print(
            f"   {row['day_name'][:3]} {row['date']}: {row['predicted_visitors']:>4} visitors [{row['staffing_level']}]"
        )

    # Summary
    print("\n📊 SUMMARY:")
    print(f"   Total: {results['predicted_visitors'].sum():,} visitors")
    print(f"   Daily avg: {results['predicted_visitors'].mean():,.0f}")
    peak = results.loc[results["predicted_visitors"].idxmax()]
    print(
        f"   Peak: {peak['day_name']} {peak['date']} ({peak['predicted_visitors']:,})"
    )

    # Log predictions
    session.sql(
        """
        CREATE TABLE IF NOT EXISTS SKI_RESORT_DB.MARTS.ML_PREDICTION_LOG (
            prediction_id NUMBER AUTOINCREMENT,
            model_name VARCHAR,
            model_version VARCHAR,
            input_date DATE,
            predicted_visitors NUMBER,
            features_json VARIANT,
            request_source VARCHAR,
            prediction_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """
    ).collect()

    logged = 0
    for idx, (_, row) in enumerate(results.iterrows()):
        try:
            features_json = json.dumps(
                {k: float(v) for k, v in input_df.iloc[idx].to_dict().items()}
            )
            session.sql(
                f"""
                INSERT INTO SKI_RESORT_DB.MARTS.ML_PREDICTION_LOG
                (model_name, model_version, input_date, predicted_visitors, features_json, request_source)
                SELECT 'VISITOR_FORECASTER', '{model_version.version_name}', '{row['date']}',
                       {row['predicted_visitors']}, PARSE_JSON($${features_json}$$), 'batch_script'
            """
            ).collect()
            logged += 1
        except Exception as e:
            print(f"   Log error: {e}")

    print(f"\n✅ Logged {logged} predictions to ML_PREDICTION_LOG")

    # Create visualization
    fig = create_visualization(results, forecast_start, FORECAST_DAYS)
    fig.savefig("forecast_output.png", dpi=150, bbox_inches="tight")
    print("📈 Saved visualization: forecast_output.png")

    session.close()
    print("\n" + "=" * 60)
    print("✅ BATCH FORECAST COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
