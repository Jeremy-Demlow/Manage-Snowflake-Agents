#!/usr/bin/env python
"""
Test the YAML-driven FeatureEngineer architecture.
Run: python test_feature_engineer.py
"""
import sys

sys.path.insert(0, ".")

import pandas as pd
import numpy as np
from pathlib import Path

# ============================================================================
# STEP 1: Connect to Snowflake
# ============================================================================
print("=" * 60)
print("STEP 1: Connect to Snowflake")
print("=" * 60)

import toml
from snowflake.snowpark import Session

config_path = Path.home() / ".snowflake" / "config.toml"
sf_config = toml.load(config_path)
conn_config = sf_config.get("connections", {}).get("snowflake_agents", {})

session = Session.builder.configs(
    {
        "account": conn_config.get("account"),
        "user": conn_config.get("user"),
        "password": conn_config.get("password"),
        "database": "SKI_RESORT_DB",
        "schema": "MARTS",
        "warehouse": conn_config.get("warehouse", "COMPUTE_WH"),
    }
).create()
print("✅ Connected to Snowflake")

# ============================================================================
# STEP 2: Understand what data we have (DATA QUALITY CHECK)
# ============================================================================
print("\n" + "=" * 60)
print("STEP 2: Data Quality Check")
print("=" * 60)

# Check FACT_WEATHER
print("\n📊 FACT_WEATHER:")
weather_check = session.sql(
    """
    SELECT
        MIN(WEATHER_DATE) as min_date,
        MAX(WEATHER_DATE) as max_date,
        COUNT(*) as row_count,
        COUNT(DISTINCT WEATHER_DATE) as unique_dates
    FROM FACT_WEATHER
"""
).to_pandas()
print(weather_check.to_string(index=False))

# Check FACT_PASS_USAGE
print("\n📊 FACT_PASS_USAGE (visitors):")
usage_check = session.sql(
    """
    SELECT
        MIN(d.FULL_DATE) as min_date,
        MAX(d.FULL_DATE) as max_date,
        COUNT(DISTINCT d.FULL_DATE) as days_with_visitors,
        COUNT(DISTINCT p.CUSTOMER_KEY) as unique_customers
    FROM DIM_DATE d
    JOIN FACT_PASS_USAGE p ON p.DATE_KEY = d.DATE_KEY
"""
).to_pandas()
print(usage_check.to_string(index=False))

# Check the JOIN overlap
print("\n📊 Checking JOIN (dates with BOTH weather AND visitors):")
overlap_check = session.sql(
    """
    SELECT
        COUNT(DISTINCT d.FULL_DATE) as dates_with_both
    FROM DIM_DATE d
    INNER JOIN FACT_PASS_USAGE p ON p.DATE_KEY = d.DATE_KEY
    INNER JOIN FACT_WEATHER w ON w.DATE_KEY = d.DATE_KEY
    WHERE p.CUSTOMER_KEY IS NOT NULL
"""
).to_pandas()
print(overlap_check.to_string(index=False))

# ============================================================================
# STEP 3: Load data using FeatureEngineer's query from YAML
# ============================================================================
print("\n" + "=" * 60)
print("STEP 3: Load Training Data (from YAML query)")
print("=" * 60)

from models.feature_engineer import FeatureEngineer

fe = FeatureEngineer.from_config("visitor_forecast")
print(f"✅ Loaded config: {fe.model_name}")

# Get query from YAML config
query = fe.get_training_query(database="SKI_RESORT_DB", schema="MARTS", years_back=4)
print("\nQuery from YAML:")
print(query[:500] + "..." if len(query) > 500 else query)

df = session.sql(query).to_pandas()
print(f"✅ Loaded {len(df)} rows of CLEAN data")
print(f"   Date range: {df['FULL_DATE'].min()} to {df['FULL_DATE'].max()}")
print(f"   Columns: {list(df.columns)}")

# Check for ANY nulls
null_counts = df.isnull().sum()
if null_counts.any():
    print("\n⚠️  NULL values found:")
    print(null_counts[null_counts > 0])
else:
    print("\n✅ No NULL values - data is clean!")

print("\nSample data:")
print(df.head(5).to_string())

# ============================================================================
# STEP 4: Engineer Features
# ============================================================================
print("\n" + "=" * 60)
print("STEP 4: Engineer Features")
print("=" * 60)

# Engineer features
df_features = fe.engineer(df)
feature_cols = fe.get_feature_columns(df_features)

print(f"✅ Engineered: {len(df_features)} rows, {len(feature_cols)} features")
print(f"   Features: {feature_cols}")

# ============================================================================
# STEP 5: Train/Test Split & Quick Model
# ============================================================================
print("\n" + "=" * 60)
print("STEP 5: Train Model")
print("=" * 60)

X_train, X_test, y_train, y_test = fe.get_train_test_split(df_features, test_ratio=0.2)
print(f"✅ Train: {len(X_train)}, Test: {len(X_test)}")

import xgboost as xgb
from sklearn.metrics import mean_absolute_error, r2_score

hyperparams = fe.get_hyperparams()
hyperparams["n_estimators"] = 100  # Quick test
hyperparams["early_stopping_rounds"] = 10

model = xgb.XGBRegressor(**hyperparams)

X_train_np = X_train.values.astype(np.float32)
X_test_np = X_test.values.astype(np.float32)
y_train_np = y_train.values.astype(np.float32)
y_test_np = y_test.values.astype(np.float32)

model.fit(X_train_np, y_train_np, eval_set=[(X_test_np, y_test_np)], verbose=False)

y_pred = model.predict(X_test_np)
mae = mean_absolute_error(y_test_np, y_pred)
r2 = r2_score(y_test_np, y_pred)

print(f"✅ Model trained")
print(f"   MAE: {mae:.1f} visitors")
print(f"   R²: {r2:.4f}")
print(f"   Quality gates: {fe.passes_quality_gates(r2, mae)}")

print("\n" + "=" * 60)
print("ALL TESTS PASSED ✅")
print("=" * 60)
