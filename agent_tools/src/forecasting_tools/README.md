# Forecasting Tools

ML-powered visitor forecasting for ski resort operations, integrated with Snowflake Cortex Agents.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    SNOWFLAKE MODEL REGISTRY                             │
│                  SKI_RESORT_DB.ML.VISITOR_FORECASTER                    │
│                        (XGBoost Model)                                  │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     SNOWPARK PROCEDURE                                  │
│           SKI_RESORT_DB.MARTS.FORECAST_VISITORS_ML                      │
│  • Loads model from registry                                            │
│  • Engineers features (YAML-driven)                                     │
│  • Makes predictions with confidence intervals                          │
│  • Logs to ML_PREDICTION_LOG                                            │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      CORTEX AGENTS                                      │
│   SKI_OPS_ASSISTANT_DEV | RESORT_EXECUTIVE_DEV                          │
│   "How many visitors next Saturday?" → forecast_visitors_ml()           │
└─────────────────────────────────────────────────────────────────────────┘
```

## Directory Structure

```
forecasting_tools/
├── forecast_handler.py          # Snowpark procedure handler
├── config/
│   ├── model_config.yaml        # Model settings & staffing thresholds
│   ├── queries.yaml             # SQL queries for data loading
│   └── features/
│       └── visitor_forecast.yaml  # Feature engineering config
├── models/
│   ├── feature_engineer.py      # YAML-driven feature engineering
│   └── config.py                # Config & query builder utilities
├── notebooks/
│   ├── train_visitor_forecast.ipynb   # Train XGBoost model
│   ├── validate_model.ipynb           # Validate model performance
│   ├── predict_batch.ipynb            # Batch predictions
│   └── snowflake_connection.py        # Connection utilities
├── scripts/
│   ├── run_batch_forecast.py    # Run batch forecasts
│   └── run_model_validation.py  # Run model validation
├── tests/
│   └── test_feature_engineer.py # Feature engineering tests
└── docs/
    ├── ML_TOOLS_PLAN.md         # Architecture documentation
    └── SNOWFLAKE_ML_NOTES.md    # Snowflake ML learning notes
```

## Quick Start

### 1. Train the Model (Notebook)

```bash
# Open the training notebook
jupyter notebook notebooks/train_visitor_forecast.ipynb
```

The notebook:
- Loads historical visitor data from Snowflake
- Engineers features using YAML-driven `FeatureEngineer`
- Trains XGBoost model with cross-validation
- Registers model to Snowflake Model Registry
- Sets DEFAULT version for production use

### 2. Deploy the Procedure

```bash
# From agent_tools/ directory
cd /path/to/agent_tools
snow snowpark deploy --connection snowflake_agents
```

This deploys `FORECAST_VISITORS_ML` procedure using `snowflake.yml`.

### 3. Test the Procedure

```sql
-- Forecast visitors for next Saturday
CALL SKI_RESORT_DB.MARTS.FORECAST_VISITORS_ML('2025-12-06');

-- Check prediction logs
SELECT * FROM SKI_RESORT_DB.MARTS.ML_PREDICTION_LOG
ORDER BY PREDICTION_TIMESTAMP DESC LIMIT 10;
```

### 4. Use via Agent

Once deployed, agents can call the procedure naturally:

> "How many visitors should we expect next Saturday?"
> "Forecast attendance for the holiday weekend"

## Feature Engineering

Features are defined in `config/features/visitor_forecast.yaml`:

```yaml
feature_engineering:
  lags: [7, 14, 21, 28]           # Historical lag features
  rolling_windows: [7, 14, 30]    # Rolling averages
  include_day_dummies: true       # One-hot encode day of week
  include_month_dummies: true     # One-hot encode month
  target_column: UNIQUE_VISITORS  # What we're predicting
```

The `FeatureEngineer` class reads this config and applies consistent transformations across:
- Training (notebook)
- Batch prediction (notebook)
- Real-time prediction (procedure)

## Configuration

### Model Config (`config/model_config.yaml`)

```yaml
model:
  name: VISITOR_FORECASTER
  database: SKI_RESORT_DB
  schema: ML

prediction:
  confidence_level: 0.80
  min_visitors: 100
  max_visitors: 10000

staffing:
  thresholds:
    high: 3000
    medium: 2000
    low: 1000
```

### Queries (`config/queries.yaml`)

SQL queries for loading training data and making predictions are centralized here.

## Model Performance

Current model (XGBoost v1.0):
- **MAE**: ~180 visitors
- **MAPE**: ~12%
- **R²**: 0.85

## Scripts

### Batch Forecast
```bash
cd scripts
python run_batch_forecast.py --start-date 2025-01-01 --days 30
```

### Model Validation
```bash
cd scripts
python run_model_validation.py
```

## Testing

```bash
cd forecasting_tools
python -m pytest tests/
```

## Deployment Notes

The procedure is deployed via Snowpark (see `agent_tools/snowflake.yml`):
- Uses `SNOWPARK_WH` (Snowpark-optimized warehouse)
- Requires XGBoost 3.0.1 (must match training environment)
- Uses `resource_constraint: {architecture: 'x86'}` for binary packages

## Troubleshooting

### "Model not found" Error
- Ensure model is registered: `SHOW MODELS IN SKI_RESORT_DB.ML`
- Check DEFAULT version is set: `SHOW VERSIONS IN MODEL VISITOR_FORECASTER`

### Feature Mismatch Error
- Retrain model if features changed
- Ensure `visitor_forecast.yaml` matches training config

### XGBoost Serialization Error
- Local and Snowflake XGBoost versions must match exactly (3.0.1)
- Reinstall if needed: `pip install xgboost==3.0.1`

---

**Status**: Production Ready | **Model**: XGBoost | **Last Updated**: Dec 2025
