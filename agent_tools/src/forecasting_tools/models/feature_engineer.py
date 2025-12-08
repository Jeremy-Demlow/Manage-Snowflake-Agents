# ============================================================================
# Generic Feature Engineer
# ============================================================================
# Scalable feature engineering that reads from YAML configs
# Works for any model type - visitor, wait_time, staffing, etc.
#
# Usage:
#   from models.feature_engineer import FeatureEngineer
#
#   fe = FeatureEngineer.from_config("visitor_forecast")
#   df = fe.engineer(raw_df)
#   feature_cols = fe.get_feature_columns()
# ============================================================================

import yaml
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple, Union


# Config directory - handle both local and Snowflake zip environments
def _get_possible_config_paths(filename: str) -> list:
    """Get list of possible config file paths."""
    base_paths = [
        # Local development path
        Path(__file__).parent.parent / "config" / "features" / filename,
        # Snowflake zip path variants
        Path(__file__).parent.parent.parent
        / "forecasting_tools"
        / "config"
        / "features"
        / filename,
    ]

    # Also try paths based on import location
    import sys

    for path in sys.path:
        if "src.zip" in path or "forecasting_tools" in path:
            base_paths.append(
                Path(path) / "forecasting_tools" / "config" / "features" / filename
            )

    return base_paths


def _deep_merge(base: Dict, override: Dict) -> Dict:
    """
    Deep merge two dictionaries. Override values take precedence.
    Lists are replaced, not merged.
    """
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_yaml(filename: str) -> Dict:
    """Load a YAML config file - handles both local and Snowflake zip environments."""
    # Try all possible paths
    for filepath in _get_possible_config_paths(filename):
        try:
            # Try to open directly - don't use exists() which fails in zip
            with open(filepath, "r") as f:
                return yaml.safe_load(f)
        except (FileNotFoundError, OSError):
            continue

    # Try using importlib.resources for package resources
    try:
        import importlib.resources as pkg_resources

        # For Python 3.9+
        try:
            from importlib.resources import files

            config_content = (
                files("forecasting_tools.config.features") / filename
            ).read_text()
            return yaml.safe_load(config_content)
        except (AttributeError, ModuleNotFoundError, TypeError):
            pass
    except ImportError:
        pass

    # Last resort: try pkgutil
    try:
        import pkgutil

        data = pkgutil.get_data("forecasting_tools.config.features", filename)
        if data:
            return yaml.safe_load(data.decode("utf-8"))
    except (FileNotFoundError, ModuleNotFoundError, TypeError):
        pass

    raise FileNotFoundError(
        f"Config not found: {filename} (tried paths and importlib.resources)"
    )


@dataclass
class FeatureEngineer:
    """
    Generic feature engineer that builds features from YAML config.

    Attributes:
        model_name: Name of the model (e.g., VISITOR_FORECASTER)
        target_column: Column to predict
        date_column: Date column name
        feature_specs: List of feature specifications
        lag_config: Lag feature configuration
        rolling_config: Rolling feature configuration
        hyperparams: Model hyperparameters
        quality_gates: Model quality thresholds
        training_query: SQL query template
    """

    model_name: str
    target_column: str
    date_column: str = "FULL_DATE"
    feature_specs: List[Dict] = field(default_factory=list)
    lag_config: Dict = field(default_factory=dict)
    rolling_config: Dict = field(default_factory=dict)
    hyperparams: Dict = field(default_factory=dict)
    quality_gates: Dict = field(default_factory=dict)
    training_query: str = ""

    # Cached feature columns
    _feature_columns: List[str] = field(default_factory=list, repr=False)

    @classmethod
    def from_config(cls, model_key: str, env: str = None) -> "FeatureEngineer":
        """
        Load feature engineer from YAML config.

        Args:
            model_key: Config file name without .yaml (e.g., "visitor_forecast")
            env: Optional environment name (e.g., "dev", "prod").
                 If provided, looks for {model_key}_{env}.yaml first,
                 falls back to {model_key}.yaml

        Supports:
            include: List of feature groups to include from base.yaml
            exclude: List of specific feature names to exclude
            override: Dict of feature_name -> properties to override
            custom_features: List of model-specific features

        Environment configs can override any setting from the base model config.
        Example: visitor_forecast_dev.yaml can have fewer features for faster iteration.
        """
        # Load base model config
        model_config = load_yaml(f"{model_key}.yaml")

        # If environment specified, try to load env-specific overrides
        if env:
            try:
                env_config = load_yaml(f"{model_key}_{env}.yaml")
                # Deep merge: env config overrides base config
                model_config = _deep_merge(model_config, env_config)
            except FileNotFoundError:
                pass  # No env-specific config, use base

        base_config = load_yaml("base.yaml")

        # Get exclusions and overrides
        exclude_features = set(model_config.get("exclude", []))
        overrides = model_config.get("override", {})

        # Build feature specs from included groups
        feature_specs = []
        include_groups = model_config.get("include", [])

        for group in include_groups:
            if group in base_config:
                for feature in base_config[group]:
                    feature_name = feature.get("name")

                    # Skip excluded features
                    if feature_name in exclude_features:
                        continue

                    # Apply overrides if any
                    if feature_name in overrides:
                        feature = {**feature, **overrides[feature_name]}

                    feature_specs.append(feature)

        # Add custom features if any
        if "custom_features" in model_config:
            for feature in model_config["custom_features"]:
                feature_name = feature.get("name")
                if feature_name not in exclude_features:
                    feature_specs.append(feature)

        model_info = model_config.get("model", {})

        # Process lag config (support exclude_lags)
        lag_config = model_config.get("lag_features", {})
        if lag_config:
            exclude_lags = set(lag_config.get("exclude_lags", []))
            if exclude_lags:
                lag_config = lag_config.copy()
                lag_config["lags"] = [
                    l for l in lag_config.get("lags", []) if l not in exclude_lags
                ]

        # Process rolling config (support exclude_windows)
        rolling_config = model_config.get("rolling_features", {})
        if rolling_config:
            exclude_windows = set(rolling_config.get("exclude_windows", []))
            if exclude_windows:
                rolling_config = rolling_config.copy()
                rolling_config["windows"] = [
                    w
                    for w in rolling_config.get("windows", [])
                    if w not in exclude_windows
                ]

        return cls(
            model_name=model_info.get("name", model_key.upper()),
            target_column=model_info.get("target_column", "TARGET"),
            date_column=model_info.get("date_column", "FULL_DATE"),
            feature_specs=feature_specs,
            lag_config=lag_config,
            rolling_config=rolling_config,
            hyperparams=model_config.get("hyperparams", {}),
            quality_gates=model_config.get("quality_gates", {}),
            training_query=model_config.get("training_query", ""),
        )

    # =========================================================================
    # FEATURE COLUMN NAMES
    # =========================================================================

    def get_feature_columns(self, df: pd.DataFrame = None) -> List[str]:
        """
        Get ordered list of feature columns.

        If df is provided, only returns columns that actually exist in the data.
        """
        cols = []

        # Features from specs
        for spec in self.feature_specs:
            cols.append(spec["name"])

        # Lag features
        if self.lag_config:
            for lag in self.lag_config.get("lags", []):
                cols.append(f"lag_{lag}")

        # Rolling features
        if self.rolling_config:
            for window in self.rolling_config.get("windows", []):
                agg = self.rolling_config.get("aggregation", "mean")
                cols.append(f"rolling_{window}_{agg}")

        # If df provided, filter to only columns that exist
        if df is not None:
            cols = [c for c in cols if c in df.columns]

        return cols

    # =========================================================================
    # FEATURE ENGINEERING
    # =========================================================================

    def engineer(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Engineer all features from raw data.

        Expects CLEAN data - no NaN values in input.
        Use INNER JOINs in your query to ensure data quality.
        """
        df = df.copy()

        # Ensure date is datetime and sorted
        df[self.date_column] = pd.to_datetime(df[self.date_column])
        df = df.sort_values(self.date_column).reset_index(drop=True)

        # Process each feature spec
        for spec in self.feature_specs:
            df = self._create_feature(df, spec)

        # Create lag features
        df = self._create_lag_features(df)

        # Create rolling features
        df = self._create_rolling_features(df)

        # Drop warmup rows (where lag features are NaN)
        max_lag = max(
            self.lag_config.get("lags", [0]) + self.rolling_config.get("windows", [0])
        )
        if max_lag > 0:
            df = df.iloc[max_lag:].reset_index(drop=True)

        return df

    def _create_feature(self, df: pd.DataFrame, spec: Dict) -> pd.DataFrame:
        """
        Create a single feature from specification.

        If source data is missing, feature is NOT created (no defaults).
        """
        name = spec["name"]
        ftype = spec.get("type", "column")

        if ftype == "date_extract":
            # Date extracts always work - we have the date column
            extract = spec.get("extract", "dayofweek")
            df[name] = getattr(df[self.date_column].dt, extract)

        elif ftype == "derived":
            # Derived features need base features to exist first
            pass

        elif ftype == "column":
            source = spec.get("source", name.upper())
            if source in df.columns:
                df[name] = df[source]
            # If source not in data, feature is NOT created

        elif ftype == "categorical":
            source = spec.get("source")
            mapping = spec.get("mapping", {})
            if source in df.columns:
                df[name] = df[source].map(mapping)
            # If source not in data, feature is NOT created

        elif ftype == "cyclical":
            source = spec.get("source")
            period = spec.get("period", 7)
            func = spec.get("function", "sin")
            if source in df.columns:
                if func == "sin":
                    df[name] = np.sin(2 * np.pi * df[source] / period)
                else:
                    df[name] = np.cos(2 * np.pi * df[source] / period)
            # If source not in data, feature is NOT created

        elif ftype == "one_hot":
            source = spec.get("source")
            value = spec.get("value")
            if source in df.columns:
                df[name] = (df[source] == value).astype(float)
            # If source not in data, feature is NOT created

        # Handle dtype conversion (expects clean data - no NaN)
        dtype = spec.get("dtype")
        if dtype and name in df.columns:
            df[name] = df[name].astype(dtype)

        return df

    def _create_lag_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create lag features from config."""
        if not self.lag_config:
            return df

        source = self.lag_config.get("source", self.target_column)
        lags = self.lag_config.get("lags", [])

        if source not in df.columns:
            return df

        for lag in lags:
            df[f"lag_{lag}"] = df[source].shift(lag)

        return df

    def _create_rolling_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create rolling features from config."""
        if not self.rolling_config:
            return df

        source = self.rolling_config.get("source", self.target_column)
        windows = self.rolling_config.get("windows", [])
        agg = self.rolling_config.get("aggregation", "mean")

        if source not in df.columns:
            return df

        for window in windows:
            col_name = f"rolling_{window}_{agg}"
            if agg == "mean":
                df[col_name] = df[source].rolling(window=window, min_periods=1).mean()
            elif agg == "sum":
                df[col_name] = df[source].rolling(window=window, min_periods=1).sum()
            elif agg == "std":
                df[col_name] = df[source].rolling(window=window, min_periods=1).std()

        return df

    # =========================================================================
    # PREDICTION FEATURES
    # =========================================================================

    def build_future_features(
        self,
        start_date: Union[str, datetime],
        days_ahead: int = 7,
        recent_values: Dict[str, float] = None,
        weather_forecast: Dict[str, Any] = None,
    ) -> pd.DataFrame:
        """
        Build features for future dates (prediction).

        Args:
            start_date: First date to predict
            days_ahead: Number of days
            recent_values: Recent averages for lag features
            weather_forecast: Weather forecast data
        """
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date)

        recent_values = recent_values or {}
        weather_forecast = weather_forecast or {}

        rows = []
        for i in range(days_ahead):
            date = start_date + timedelta(days=i)
            row = {self.date_column: date}

            # Build each feature
            for spec in self.feature_specs:
                name = spec["name"]
                ftype = spec.get("type", "column")

                if ftype == "date_extract":
                    extract = spec.get("extract", "dayofweek")
                    # Convert to pandas Timestamp for consistent API
                    ts = pd.Timestamp(date)
                    if extract == "dayofweek":
                        row[name] = ts.dayofweek
                    elif extract == "month":
                        row[name] = ts.month
                    else:
                        row[name] = getattr(ts, extract, 0)

                elif ftype == "cyclical":
                    source = spec.get("source")
                    period = spec.get("period", 7)
                    func = spec.get("function", "sin")
                    source_val = row.get(source, 0)
                    if func == "sin":
                        row[name] = np.sin(2 * np.pi * source_val / period)
                    else:
                        row[name] = np.cos(2 * np.pi * source_val / period)

                elif ftype == "one_hot":
                    source = spec.get("source")
                    value = spec.get("value")
                    row[name] = 1.0 if row.get(source) == value else 0.0

                elif ftype == "column":
                    # Use weather forecast or default
                    source = spec.get("source", name.upper())
                    default = spec.get("default", 0)
                    row[name] = weather_forecast.get(source, default)

                elif ftype == "categorical":
                    source = spec.get("source")
                    mapping = spec.get("mapping", {})
                    default = spec.get("default", 0)
                    weather_val = weather_forecast.get(source)
                    row[name] = mapping.get(weather_val, default)

            # Lag features (use recent values with proper keys)
            if self.lag_config:
                for lag in self.lag_config.get("lags", []):
                    key = f"lag_{lag}"
                    row[key] = recent_values.get(key, 0)

            # Rolling features (use recent values with proper keys)
            if self.rolling_config:
                agg = self.rolling_config.get("aggregation", "mean")
                for window in self.rolling_config.get("windows", []):
                    key = f"rolling_{window}_{agg}"
                    row[key] = recent_values.get(key, 0)

            rows.append(row)

        return pd.DataFrame(rows)

    # =========================================================================
    # UTILITIES
    # =========================================================================

    def get_train_test_split(
        self, df: pd.DataFrame, test_ratio: float = 0.2
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
        """Time-based train/test split."""
        feature_cols = self.get_feature_columns()

        split_idx = int(len(df) * (1 - test_ratio))
        train_df = df.iloc[:split_idx]
        test_df = df.iloc[split_idx:]

        return (
            train_df[feature_cols],
            test_df[feature_cols],
            train_df[self.target_column],
            test_df[self.target_column],
        )

    def to_model_input(self, df: pd.DataFrame) -> np.ndarray:
        """Convert DataFrame to numpy array for model."""
        feature_cols = self.get_feature_columns()
        return df[feature_cols].values.astype(np.float32)

    def get_training_query(self, **params) -> str:
        """Get training query with parameters substituted."""
        return self.training_query.format(**params)

    def get_hyperparams(self, **overrides) -> Dict[str, Any]:
        """Get hyperparams with optional overrides."""
        params = self.hyperparams.copy()
        params.update(overrides)
        return params

    def passes_quality_gates(self, r2: float, mae: float, mape: float = None) -> bool:
        """Check if metrics pass quality gates."""
        if not self.quality_gates:
            return True
        passes = r2 >= self.quality_gates.get("min_r2", 0)
        passes = passes and mae <= self.quality_gates.get("max_mae", float("inf"))
        if mape is not None and "max_mape" in self.quality_gates:
            passes = passes and mape <= self.quality_gates["max_mape"]
        return passes

    def describe(self, verbose: bool = False) -> str:
        """
        Describe the feature engineer configuration.

        Args:
            verbose: If True, list all feature names
        """
        feature_cols = self.get_feature_columns()

        # Group features by type for summary
        feature_types = {}
        for spec in self.feature_specs:
            ftype = spec.get("type", "column")
            if ftype not in feature_types:
                feature_types[ftype] = []
            feature_types[ftype].append(spec.get("name"))

        desc = f"""
Feature Engineer: {self.model_name}
  Target: {self.target_column}
  Date Column: {self.date_column}
  Total Features: {len(feature_cols)}

  Feature Types:
"""
        for ftype, names in sorted(feature_types.items()):
            desc += f"    {ftype}: {len(names)} features\n"
            if verbose:
                desc += f"      → {', '.join(names[:5])}"
                if len(names) > 5:
                    desc += f" ... (+{len(names)-5} more)"
                desc += "\n"

        desc += f"""
  Lag Features: {self.lag_config.get('lags', [])}
  Rolling Features: {self.rolling_config.get('windows', [])} ({self.rolling_config.get('aggregation', 'mean')})

  Quality Gates:
    R² ≥ {self.quality_gates.get('min_r2', 'N/A')}
    MAE ≤ {self.quality_gates.get('max_mae', 'N/A')}
    MAPE ≤ {self.quality_gates.get('max_mape', 'N/A')}%
"""
        return desc

    def list_features(self) -> Dict[str, List[str]]:
        """
        Get features organized by type.

        Returns dict like:
            {
                'base': ['day_of_week', 'month', ...],
                'lag': ['lag_7', 'lag_14', ...],
                'rolling': ['rolling_7_mean', ...]
            }
        """
        result = {"base": [], "lag": [], "rolling": []}

        for spec in self.feature_specs:
            result["base"].append(spec.get("name"))

        if self.lag_config:
            for lag in self.lag_config.get("lags", []):
                result["lag"].append(f"lag_{lag}")

        if self.rolling_config:
            agg = self.rolling_config.get("aggregation", "mean")
            for window in self.rolling_config.get("windows", []):
                result["rolling"].append(f"rolling_{window}_{agg}")

        return result
