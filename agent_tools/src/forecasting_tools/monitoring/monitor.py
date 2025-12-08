"""
ML Model Monitoring for Snowflake

Provides:
- Prediction logging with automatic feature capture
- Drift detection using Snowflake ML Observability
- Performance tracking and alerting
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)


@dataclass
class MonitoringConfig:
    """Configuration for model monitoring."""

    enabled: bool = True
    log_predictions: bool = True
    log_features: bool = True
    mae_warning: float = 150
    mae_critical: float = 250
    mape_warning: float = 25
    mape_critical: float = 40
    drift_zscore: float = 2.0

    @classmethod
    def from_yaml(cls, config_path: str = None) -> "MonitoringConfig":
        """Load monitoring config from model_config.yaml."""
        if config_path is None:
            import os

            config_path = os.path.join(
                os.path.dirname(__file__), "..", "config", "model_config.yaml"
            )

        try:
            with open(config_path) as f:
                config = yaml.safe_load(f)

            mon = config.get("monitoring", {})
            thresholds = mon.get("thresholds", {})

            return cls(
                enabled=mon.get("enabled", True),
                log_predictions=mon.get("log_predictions", True),
                log_features=mon.get("log_features", True),
                mae_warning=thresholds.get("mae_warning", 150),
                mae_critical=thresholds.get("mae_critical", 250),
                mape_warning=thresholds.get("mape_warning", 25),
                mape_critical=thresholds.get("mape_critical", 40),
                drift_zscore=thresholds.get("drift_zscore", 2.0),
            )
        except Exception as e:
            logger.warning(f"Could not load monitoring config: {e}, using defaults")
            return cls()


class PredictionLogger:
    """
    Logs model predictions for observability.

    Stores predictions with features so we can:
    - Track accuracy over time (when actuals arrive)
    - Detect feature drift
    - Debug model issues
    """

    def __init__(self, session, config: MonitoringConfig = None):
        """
        Initialize logger with Snowpark session.

        Args:
            session: Snowpark Session
            config: Optional MonitoringConfig (loads from yaml if not provided)
        """
        self.session = session
        self.config = config or MonitoringConfig.from_yaml()
        self._table = "SKI_RESORT_DB.MODELS.ML_PREDICTION_LOG"

    def log_prediction(
        self,
        model_name: str,
        model_version: str,
        forecast_date: str,
        predicted_visitors: int,
        features: Dict[str, Any] = None,
        request_source: str = "agent",
    ) -> bool:
        """
        Log a single prediction.

        Args:
            model_name: Name of the model (e.g., VISITOR_FORECASTER)
            model_version: Version string
            forecast_date: Date being predicted (YYYY-MM-DD)
            predicted_visitors: The prediction value
            features: Dict of key features for drift detection
            request_source: Where the request came from (agent, api, batch)

        Returns:
            True if logged successfully
        """
        if not self.config.enabled or not self.config.log_predictions:
            return True

        features = features or {}

        try:
            # Extract features we track for drift
            day_of_week = features.get("day_of_week")
            month = features.get("month")
            is_weekend = features.get("is_weekend")
            is_holiday = features.get("is_holiday", False)
            lag_7 = features.get("lag_7")
            lag_14 = features.get("lag_14")
            rolling_7_mean = features.get("rolling_7_mean")

            sql = f"""
                INSERT INTO {self._table} (
                    model_name, model_version, forecast_date, predicted_visitors,
                    day_of_week, month, is_weekend, is_holiday,
                    lag_7, lag_14, rolling_7_mean, request_source
                )
                VALUES (
                    '{model_name}', '{model_version}', '{forecast_date}', {predicted_visitors},
                    {day_of_week if day_of_week is not None else 'NULL'},
                    {month if month is not None else 'NULL'},
                    {is_weekend if is_weekend is not None else 'NULL'},
                    {is_holiday},
                    {lag_7 if lag_7 is not None else 'NULL'},
                    {lag_14 if lag_14 is not None else 'NULL'},
                    {rolling_7_mean if rolling_7_mean is not None else 'NULL'},
                    '{request_source}'
                )
            """
            self.session.sql(sql).collect()
            logger.debug(f"Logged prediction for {forecast_date}: {predicted_visitors}")
            return True

        except Exception as e:
            logger.warning(f"Failed to log prediction: {e}")
            return False

    def log_batch(
        self,
        model_name: str,
        model_version: str,
        predictions: List[Dict[str, Any]],
        request_source: str = "agent",
    ) -> int:
        """
        Log multiple predictions at once.

        Args:
            model_name: Model name
            model_version: Version string
            predictions: List of dicts with 'forecast_date', 'predicted_visitors', 'features'
            request_source: Request source

        Returns:
            Number of predictions logged successfully
        """
        logged = 0
        for pred in predictions:
            if self.log_prediction(
                model_name=model_name,
                model_version=model_version,
                forecast_date=pred["forecast_date"],
                predicted_visitors=pred["predicted_visitors"],
                features=pred.get("features", {}),
                request_source=request_source,
            ):
                logged += 1
        return logged


class ModelMonitor:
    """
    Monitor model performance and drift.

    Uses Snowflake ML Observability when available,
    falls back to custom queries otherwise.
    """

    def __init__(self, session, config: MonitoringConfig = None):
        self.session = session
        self.config = config or MonitoringConfig.from_yaml()

    def get_recent_performance(self, days: int = 7) -> Dict[str, Any]:
        """
        Get performance metrics for recent predictions.

        Returns:
            Dict with mae, mape, prediction_count, etc.
        """
        sql = f"""
            SELECT
                COUNT(*) as prediction_count,
                AVG(ABS(predicted_visitors - actual_visitors)) as mae,
                AVG(ABS(predicted_visitors - actual_visitors) / NULLIF(actual_visitors, 0) * 100) as mape,
                MAX(ABS(predicted_visitors - actual_visitors)) as max_error
            FROM SKI_RESORT_DB.MODELS.ML_PREDICTION_LOG
            WHERE actual_visitors IS NOT NULL
              AND prediction_timestamp >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
        """
        try:
            result = self.session.sql(sql).collect()
            if result:
                row = result[0]
                return {
                    "prediction_count": row["PREDICTION_COUNT"],
                    "mae": float(row["MAE"]) if row["MAE"] else None,
                    "mape": float(row["MAPE"]) if row["MAPE"] else None,
                    "max_error": float(row["MAX_ERROR"]) if row["MAX_ERROR"] else None,
                    "status": self._evaluate_status(row["MAE"], row["MAPE"]),
                }
            return {"prediction_count": 0, "status": "no_data"}
        except Exception as e:
            logger.error(f"Failed to get performance: {e}")
            return {"error": str(e)}

    def _evaluate_status(self, mae: float, mape: float) -> str:
        """Evaluate model status based on thresholds."""
        if mae is None or mape is None:
            return "unknown"
        if mae > self.config.mae_critical or mape > self.config.mape_critical:
            return "critical"
        if mae > self.config.mae_warning or mape > self.config.mape_warning:
            return "warning"
        return "healthy"

    def check_drift(self) -> Dict[str, Any]:
        """
        Check for feature drift comparing recent vs baseline.

        Returns:
            Dict with drift metrics per feature
        """
        sql = """
            WITH recent AS (
                SELECT
                    AVG(lag_7) as avg_lag_7,
                    STDDEV(lag_7) as std_lag_7,
                    AVG(rolling_7_mean) as avg_rolling
                FROM SKI_RESORT_DB.MODELS.ML_PREDICTION_LOG
                WHERE prediction_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP())
            ),
            baseline AS (
                SELECT
                    AVG(lag_7) as avg_lag_7,
                    STDDEV(lag_7) as std_lag_7,
                    AVG(rolling_7_mean) as avg_rolling
                FROM SKI_RESORT_DB.MODELS.ML_PREDICTION_LOG
                WHERE prediction_timestamp >= DATEADD('day', -30, CURRENT_TIMESTAMP())
                  AND prediction_timestamp < DATEADD('day', -7, CURRENT_TIMESTAMP())
            )
            SELECT
                r.avg_lag_7 as recent_lag_7,
                b.avg_lag_7 as baseline_lag_7,
                CASE
                    WHEN b.std_lag_7 > 0
                    THEN ABS(r.avg_lag_7 - b.avg_lag_7) / b.std_lag_7
                    ELSE 0
                END as drift_zscore
            FROM recent r, baseline b
        """
        try:
            result = self.session.sql(sql).collect()
            if result:
                row = result[0]
                drift_z = float(row["DRIFT_ZSCORE"]) if row["DRIFT_ZSCORE"] else 0
                return {
                    "lag_7": {
                        "recent": row["RECENT_LAG_7"],
                        "baseline": row["BASELINE_LAG_7"],
                        "zscore": drift_z,
                        "drifted": drift_z > self.config.drift_zscore,
                    },
                    "status": "drift_detected"
                    if drift_z > self.config.drift_zscore
                    else "stable",
                }
            return {"status": "no_data"}
        except Exception as e:
            logger.error(f"Failed to check drift: {e}")
            return {"error": str(e)}

    def get_summary(self) -> Dict[str, Any]:
        """Get full monitoring summary."""
        return {
            "performance": self.get_recent_performance(),
            "drift": self.check_drift(),
            "timestamp": datetime.now().isoformat(),
        }


def backfill_actuals(session) -> str:
    """
    Backfill actual visitor counts for past predictions.

    Should be run daily after data pipeline completes.

    Args:
        session: Snowpark Session

    Returns:
        Status message
    """
    sql = """
        UPDATE SKI_RESORT_DB.MODELS.ML_PREDICTION_LOG p
        SET actual_visitors = (
            SELECT COUNT(DISTINCT CUSTOMER_KEY)
            FROM SKI_RESORT_DB.MARTS.FACT_PASS_USAGE f
            JOIN SKI_RESORT_DB.MARTS.DIM_DATE d ON f.DATE_KEY = d.DATE_KEY
            WHERE d.FULL_DATE = p.forecast_date
        )
        WHERE p.actual_visitors IS NULL
          AND p.forecast_date < CURRENT_DATE()
    """
    try:
        result = session.sql(sql).collect()
        return f"Backfilled actuals successfully"
    except Exception as e:
        return f"Backfill failed: {str(e)}"
