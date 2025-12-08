"""
Snowpark Python Handlers for Visitor Forecasting

Can be run from:
- Snowsight notebooks (get_active_session)
- Local Python scripts
- Snowpark Python UDFs/SPROCs (if deployed)
"""
import json
import yaml
from datetime import datetime, timedelta
from pathlib import Path
from snowflake.snowpark import Session
from snowflake.ml.registry import Registry

# Import our existing modules
from forecasting_tools.models.feature_engineer import FeatureEngineer


def _load_staffing_config():
    """Load staffing levels from YAML config."""
    try:
        config_path = Path(__file__).parent / "config" / "model_config.yaml"
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        return config.get("staffing_levels", {})
    except:
        # Fallback defaults if config not found (e.g., in Snowflake zip)
        return {
            "minimal": {
                "max_visitors": 500,
                "lift_operators": "6-8",
                "f_and_b": "1 location",
            },
            "light": {
                "max_visitors": 1000,
                "lift_operators": "10-12",
                "f_and_b": "2 locations",
            },
            "moderate": {
                "max_visitors": 2000,
                "lift_operators": "14-16",
                "f_and_b": "3 locations",
            },
            "high": {
                "max_visitors": 3000,
                "lift_operators": "18-20",
                "f_and_b": "All locations",
            },
            "maximum": {
                "max_visitors": 999999,
                "lift_operators": "22+",
                "f_and_b": "All + overflow",
            },
        }


def _get_staffing(visitors: int, staffing_levels: dict = None) -> tuple:
    """Get staffing recommendation for visitor count."""
    if staffing_levels is None:
        staffing_levels = _load_staffing_config()

    for level_name in ["minimal", "light", "moderate", "high", "maximum"]:
        level = staffing_levels.get(level_name, {})
        if visitors < level.get("max_visitors", 999999):
            return (
                level_name.title(),
                level.get("lift_operators", "TBD"),
                level.get("f_and_b", "TBD"),
            )
    return "Maximum", "22+", "All + overflow"


def forecast_visitors(
    session: Session, forecast_start_date, days_ahead: int = 7
) -> str:
    """
    Forecast daily visitors using the ML model from registry.

    Uses FeatureEngineer for consistent feature building.
    """
    import pandas as pd
    import numpy as np

    # Load feature engineer (same config used for training)
    fe = FeatureEngineer.from_config("visitor_forecast")

    # Load staffing config once
    staffing_levels = _load_staffing_config()

    # Load model from registry
    try:
        registry = Registry(
            session, database_name="SKI_RESORT_DB", schema_name="MODELS"
        )
        model = registry.get_model("VISITOR_FORECASTER")
        model_version = model.default
        version_name = model_version.version_name
    except Exception as e:
        return json.dumps(
            {
                "error": f"Model not found: {str(e)}",
                "hint": "Run train_visitor_forecast.ipynb first",
            }
        )

    # Get historical data with actual visitor counts by date
    start_date = pd.to_datetime(forecast_start_date)
    target_month = start_date.month

    # Get recent historical data for lag calculations (need at least 30 days back)
    history_query = f"""
    SELECT
        d.FULL_DATE,
        d.DAY_OF_WEEK,
        COUNT(DISTINCT p.CUSTOMER_KEY) as UNIQUE_VISITORS
    FROM SKI_RESORT_DB.MARTS.DIM_DATE d
    LEFT JOIN SKI_RESORT_DB.MARTS.FACT_PASS_USAGE p ON d.DATE_KEY = p.DATE_KEY
    WHERE d.FULL_DATE >= DATEADD('day', -60, '{start_date.strftime('%Y-%m-%d')}'::DATE)
      AND d.FULL_DATE < '{start_date.strftime('%Y-%m-%d')}'::DATE
    GROUP BY d.FULL_DATE, d.DAY_OF_WEEK
    ORDER BY d.FULL_DATE
    """
    hist_df = session.sql(history_query).to_pandas()
    hist_df["FULL_DATE"] = pd.to_datetime(hist_df["FULL_DATE"])
    hist_df = hist_df.set_index("FULL_DATE")

    # Get same-day-of-week averages for the target month (more accurate baseline)
    dow_query = f"""
    SELECT
        d.DAY_OF_WEEK,
        AVG(cnt.visitors) as avg_visitors
    FROM SKI_RESORT_DB.MARTS.DIM_DATE d
    JOIN (
        SELECT DATE_KEY, COUNT(DISTINCT CUSTOMER_KEY) as visitors
        FROM SKI_RESORT_DB.MARTS.FACT_PASS_USAGE
        GROUP BY DATE_KEY
    ) cnt ON d.DATE_KEY = cnt.DATE_KEY
    WHERE d.MONTH_NUM = {target_month}
    GROUP BY d.DAY_OF_WEEK
    """
    dow_df = session.sql(dow_query).to_pandas()
    dow_avgs = dict(zip(dow_df["DAY_OF_WEEK"], dow_df["AVG_VISITORS"]))

    # Get weather for target month
    weather_query = f"""
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
    weather_df = session.sql(weather_query).to_pandas()

    weather_forecast = {
        "snowfall_inches": float(weather_df["AVG_SNOWFALL"].iloc[0] or 5.0),
        "base_depth_inches": float(weather_df["AVG_BASE_DEPTH"].iloc[0] or 100.0),
        "avg_temp_f": float(weather_df["AVG_TEMP"].iloc[0] or 28.0),
        "wind_speed_mph": float(weather_df["AVG_WIND"].iloc[0] or 12.0),
        "is_powder_day": 1 if (weather_df["POWDER_RATE"].iloc[0] or 0) > 0.3 else 0,
        "is_high_wind": 0,
    }

    # Build features for each forecast day with ACTUAL lag values
    feature_cols = fe.get_feature_columns()
    forecast_dates = [start_date + timedelta(days=i) for i in range(int(days_ahead))]

    input_rows = []
    for forecast_date in forecast_dates:
        # Calculate actual lag values from historical data
        recent_values = {}
        for lag in [7, 14, 21, 28]:
            lag_date = forecast_date - timedelta(days=lag)
            if lag_date in hist_df.index:
                recent_values[f"lag_{lag}"] = float(
                    hist_df.loc[lag_date, "UNIQUE_VISITORS"]
                )
            else:
                # Fallback: use same-day-of-week average
                dow = lag_date.dayofweek
                recent_values[f"lag_{lag}"] = float(dow_avgs.get(dow, 400))

        # Rolling features from actual history
        for window in [7, 14, 30]:
            window_start = forecast_date - timedelta(days=window)
            window_data = hist_df[hist_df.index >= window_start]["UNIQUE_VISITORS"]
            recent_values[f"rolling_{window}_mean"] = (
                float(window_data.mean()) if len(window_data) > 0 else 400.0
            )

        # Build features for this single day
        day_df = fe.build_future_features(
            start_date=forecast_date,
            days_ahead=1,
            recent_values=recent_values,
            weather_forecast=weather_forecast,
        )
        input_rows.append(day_df.iloc[0])

    input_df = pd.DataFrame(input_rows)[feature_cols].astype(float)

    # Make predictions
    try:
        input_sp = session.create_dataframe(input_df)
        predictions_sp = model_version.run(input_sp, function_name="predict")
        predictions_df = predictions_sp.to_pandas()

        pred_col = [c for c in predictions_df.columns if "predict" in c.lower()][0]
        predicted_values = predictions_df[pred_col].values
    except Exception as e:
        return json.dumps(
            {"error": f"Prediction failed: {str(e)}", "model_version": version_name}
        )

    # Build response with staffing recommendations
    results = []
    forecast_dates = [start_date + timedelta(days=i) for i in range(int(days_ahead))]

    for i, pred_date in enumerate(forecast_dates):
        pred_visitors = int(max(0, predicted_values[i]))

        # Get staffing from config
        staffing, lift_ops, fnb = _get_staffing(pred_visitors, staffing_levels)

        results.append(
            {
                "date": pred_date.strftime("%Y-%m-%d"),
                "day": pred_date.strftime("%A"),
                "predicted_visitors": pred_visitors,
                "staffing_level": staffing,
                "lift_operators": lift_ops,
                "food_beverage": fnb,
            }
        )

        # Log prediction for ML Observability
        day_features = input_rows[i] if i < len(input_rows) else {}
        try:
            session.sql(
                f"""
                INSERT INTO SKI_RESORT_DB.MODELS.ML_PREDICTION_LOG
                (model_name, model_version, forecast_date, predicted_visitors,
                 day_of_week, month, is_weekend, lag_7, lag_14, rolling_7_mean, request_source)
                VALUES (
                    'VISITOR_FORECASTER', '{version_name}', '{pred_date.strftime('%Y-%m-%d')}',
                    {pred_visitors},
                    {int(day_features.get('day_of_week', pred_date.weekday()))},
                    {pred_date.month},
                    {pred_date.weekday() >= 5},
                    {day_features.get('lag_7', 'NULL')},
                    {day_features.get('lag_14', 'NULL')},
                    {day_features.get('rolling_7_mean', 'NULL')},
                    'agent'
                )
            """
            ).collect()
        except Exception:
            pass  # Don't fail prediction if logging fails

    return json.dumps(
        {
            "model": "VISITOR_FORECASTER",
            "version": version_name,
            "forecast_start": str(forecast_start_date),
            "days_forecasted": int(days_ahead),
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "predictions": results,
            "summary": {
                "total_visitors": sum(r["predicted_visitors"] for r in results),
                "avg_daily": int(np.mean([r["predicted_visitors"] for r in results])),
                "peak_day": max(results, key=lambda x: x["predicted_visitors"])["date"],
                "peak_visitors": max(r["predicted_visitors"] for r in results),
            },
        },
        indent=2,
    )


def forecast_next_week(session: Session) -> str:
    """Quick forecast for the next 7 days starting tomorrow."""
    tomorrow = (datetime.now() + timedelta(days=1)).date()
    return forecast_visitors(session, tomorrow, 7)
