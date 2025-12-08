# ============================================================================
# Configuration Loader
# ============================================================================
# Loads YAML configs and provides easy access to settings
# Single source of truth for all model configuration
#
# Usage:
#   from models.config import ModelConfig, QueryBuilder
#
#   config = ModelConfig.from_yaml()
#   qb = QueryBuilder()
# ============================================================================

import yaml
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional

# Get the config directory path
CONFIG_DIR = Path(__file__).parent.parent / "config"


def load_yaml(filename: str) -> Dict:
    """Load a YAML config file."""
    filepath = CONFIG_DIR / filename
    if not filepath.exists():
        raise FileNotFoundError(f"Config file not found: {filepath}")

    with open(filepath, "r") as f:
        return yaml.safe_load(f)


def load_config() -> Dict:
    """Load the main model configuration."""
    return load_yaml("model_config.yaml")


def load_queries() -> Dict:
    """Load SQL queries configuration."""
    return load_yaml("queries.yaml")


@dataclass
class ModelConfig:
    """
    Configuration object for a specific model.

    Usage:
        config = ModelConfig.from_yaml("visitor_forecaster")
        print(config.hyperparams)
        print(config.quality_gates)
    """

    name: str
    description: str
    hyperparams: Dict[str, Any]
    quality_gates: Dict[str, float]
    drift_thresholds: Dict[str, float]

    # Registry settings
    database: str = "SKI_RESORT_DB"
    schema: str = "MODELS"

    # Feature settings
    lags: List[int] = field(default_factory=lambda: [7, 14, 21, 28])
    rolling_windows: List[int] = field(default_factory=lambda: [7, 14, 30])
    include_day_dummies: bool = True
    include_month_dummies: bool = True

    # Prediction settings
    default_days_ahead: int = 7
    staffing_levels: Dict[str, Dict] = field(default_factory=dict)

    @classmethod
    def from_yaml(cls, model_key: str = "visitor_forecaster") -> "ModelConfig":
        """Load config from YAML file."""
        config = load_config()
        model_config = config["models"][model_key]
        registry = config["registry"]
        features = config["features"]
        prediction = config["prediction"]

        return cls(
            name=model_config["name"],
            description=model_config["description"],
            hyperparams=model_config["hyperparams"],
            quality_gates=model_config["quality_gates"],
            drift_thresholds=model_config["drift_thresholds"],
            database=registry["database"],
            schema=registry["schema"],
            lags=features["lags"],
            rolling_windows=features["rolling_windows"],
            include_day_dummies=features["include_day_dummies"],
            include_month_dummies=features["include_month_dummies"],
            default_days_ahead=prediction["default_days_ahead"],
            staffing_levels=prediction["staffing_levels"],
        )

    def get_hyperparams(self, **overrides) -> Dict[str, Any]:
        """Get hyperparams with optional overrides."""
        params = self.hyperparams.copy()
        params.update(overrides)
        return params

    def passes_quality_gates(self, r2: float, mae: float, mape: float = None) -> bool:
        """Check if metrics pass quality gates."""
        passes = r2 >= self.quality_gates["min_r2"]
        passes = passes and mae <= self.quality_gates["max_mae"]
        if mape is not None:
            passes = passes and mape <= self.quality_gates["max_mape"]
        return passes

    def get_staffing_level(self, visitors: int) -> Dict[str, str]:
        """Get staffing recommendation for visitor count."""
        for level, config in self.staffing_levels.items():
            if visitors < config["max_visitors"]:
                return {
                    "level": level.title(),
                    "lift_operators": config["lift_operators"],
                    "f_and_b": config["f_and_b"],
                }
        # Default to maximum
        return {
            "level": "Maximum",
            "lift_operators": "22+",
            "f_and_b": "All + overflow",
        }


class QueryBuilder:
    """
    Build SQL queries from templates with parameter substitution.

    Usage:
        qb = QueryBuilder()
        sql = qb.training_data(years_back=4)
        sql = qb.validation(model_name="VISITOR_FORECASTER")
    """

    def __init__(self, database: str = None, schema: str = None):
        self.queries_config = load_queries()
        self.database = database or self.queries_config["database"]
        self.schema = schema or self.queries_config["schema"]
        self.queries = self.queries_config["queries"]

    def _build(self, query_name: str, **params) -> str:
        """Build a query with parameter substitution."""
        if query_name not in self.queries:
            raise ValueError(f"Unknown query: {query_name}")

        query = self.queries[query_name]

        # Add default params
        all_params = {
            "database": self.database,
            "schema": self.schema,
        }
        all_params.update(params)

        return query.format(**all_params)

    def training_data(self, years_back: int = 4) -> str:
        """Get training data query."""
        return self._build("training_data", years_back=years_back)

    def historical_for_lags(self, days_back: int = 90) -> str:
        """Get historical data query for lag features."""
        return self._build("historical_for_lags", days_back=days_back)

    def validation(self, model_name: str = "VISITOR_FORECASTER") -> str:
        """Get validation query."""
        return self._build("validation", model_name=model_name)

    def log_prediction(
        self,
        model_name: str,
        model_version: str,
        input_date: str,
        predicted_visitors: int,
        features_json: str,
        request_source: str = "notebook",
    ) -> str:
        """Get log prediction query."""
        return self._build(
            "log_prediction",
            model_name=model_name,
            model_version=model_version,
            input_date=input_date,
            predicted_visitors=predicted_visitors,
            features_json=features_json,
            request_source=request_source,
        )
