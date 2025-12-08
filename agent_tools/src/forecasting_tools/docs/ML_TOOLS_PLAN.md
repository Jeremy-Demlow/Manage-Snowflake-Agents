# ML-Powered Agent Tools - Implementation Plan

## Overview

Transform the forecasting_tools from rule-based heuristics to ML-powered predictions using **Snowflake Model Registry**. This enables:

1. **Better predictions** - ML models trained on historical ski resort data
2. **Model versioning** - Track model performance over time
3. **Agent integration** - SQL procedures wrap model calls for agent use
4. **Observability** - Track model predictions and accuracy

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    SNOWFLAKE MODEL REGISTRY                         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐  │
│  │ VISITOR_FORECAST│  │ WAIT_TIME_MODEL │  │ STAFFING_OPTIMIZER  │  │
│  │ (Time-Series)   │  │ (Regression)    │  │ (Regression)        │  │
│  └────────┬────────┘  └────────┬────────┘  └──────────┬──────────┘  │
└───────────┼──────────────────────────────────────────┼──────────────┘
            │                    │                      │
            ▼                    ▼                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     SQL WRAPPER PROCEDURES                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐  │
│  │ FORECAST_VISITORS│ │PREDICT_WAIT_TIME│  │OPTIMIZE_STAFFING_ML │  │
│  │ (Python Proc)   │  │ (Python Proc)   │  │ (Python Proc)       │  │
│  └────────┬────────┘  └────────┬────────┘  └──────────┬──────────┘  │
└───────────┼──────────────────────────────────────────┼──────────────┘
            │                    │                      │
            ▼                    ▼                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      CORTEX AGENTS                                  │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  Ski Operations Assistant                                    │   │
│  │  - "How many visitors can we expect next Saturday?"          │   │
│  │  - "What will wait times be at Summit Gondola tomorrow?"     │   │
│  │  - "How should we staff for the holiday weekend?"            │   │
│  └─────────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  Resort Executive Assistant                                  │   │
│  │  - "Forecast revenue for next week"                          │   │
│  │  - "What's the expected visitor trend for January?"          │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## ML Models to Build

### 1. VISITOR_FORECASTER 🎯

**Purpose:** Predict daily visitor counts for staffing and revenue planning

**Model Type:** Gradient Boosting Regressor (scikit-learn) or Cortex ML FORECAST

**Features:**
| Feature | Type | Source |
|---------|------|--------|
| `day_of_week` | Categorical | Date |
| `month` | Categorical | Date |
| `is_weekend` | Boolean | Date |
| `is_holiday` | Boolean | DIM_DATE |
| `days_since_last_snowfall` | Numeric | Weather |
| `recent_snowfall_inches` | Numeric | Weather |
| `snow_condition` | Categorical | Weather |
| `lag_7_visitors` | Numeric | Historical |
| `lag_30_avg_visitors` | Numeric | Historical |

**Target:** `unique_visitors` (count per day)

**Training Data:** 4 seasons of daily visitor counts (~500 rows)

**Expected Accuracy:** MAE < 300 visitors (±15%)

---

### 2. WAIT_TIME_PREDICTOR ⏱️

**Purpose:** Predict lift wait times for operations planning

**Model Type:** Gradient Boosting Regressor

**Features:**
| Feature | Type | Source |
|---------|------|--------|
| `lift_type` | Categorical | DIM_LIFT |
| `terrain_type` | Categorical | DIM_LIFT |
| `hour_of_day` | Numeric | Time |
| `day_of_week` | Categorical | Date |
| `is_weekend` | Boolean | Date |
| `is_holiday` | Boolean | Date |
| `expected_visitors` | Numeric | From VISITOR_FORECASTER |
| `capacity_per_hour` | Numeric | DIM_LIFT |

**Target:** `avg_wait_minutes` per lift

**Training Data:** Historical lift scans aggregated by lift/hour (~50K rows)

**Expected Accuracy:** MAE < 3 minutes

---

### 3. STAFFING_OPTIMIZER 👷

**Purpose:** Recommend staffing levels by department

**Model Type:** Multi-output Regressor

**Features:**
| Feature | Type | Source |
|---------|------|--------|
| `expected_visitors` | Numeric | From VISITOR_FORECASTER |
| `day_of_week` | Categorical | Date |
| `is_weekend` | Boolean | Date |
| `is_holiday` | Boolean | Date |
| `historical_coverage` | Numeric | DIM_STAFFING |

**Targets (multi-output):**
- `lift_ops_staff_needed`
- `fnb_staff_needed`
- `rental_staff_needed`
- `guest_services_needed`
- `ski_patrol_needed`

**Training Data:** Historical staffing with visitor counts (~1,750 rows)

---

### 4. REVENUE_FORECASTER 💰 (Bonus)

**Purpose:** Predict daily revenue by category

**Model Type:** Multi-output Regressor

**Features:**
| Feature | Type | Source |
|---------|------|--------|
| `expected_visitors` | Numeric | From VISITOR_FORECASTER |
| `day_of_week` | Categorical | Date |
| `is_weekend` | Boolean | Date |
| `is_holiday` | Boolean | Date |
| `weather_condition` | Categorical | Weather |
| `pass_holder_pct` | Numeric | Historical |

**Targets:**
- `ticket_revenue`
- `rental_revenue`
- `fnb_revenue`
- `total_revenue`

---

## Implementation Files

### 1. Notebook: `train_and_register_models.ipynb`

**Location:** `agent_tools/src/forecasting_tools/notebooks/`

**Contents:**
```python
# Cell 1: Setup & Imports
# Cell 2: Load Historical Data
# Cell 3: Feature Engineering
# Cell 4: Train VISITOR_FORECASTER
# Cell 5: Train WAIT_TIME_PREDICTOR  
# Cell 6: Train STAFFING_OPTIMIZER
# Cell 7: Register Models in Model Registry
# Cell 8: Test Model Inference
# Cell 9: Log Model Metadata
```

**Outputs:**
- Models registered in `SKI_RESORT_DB.MODELS`
- Training metrics logged
- Feature importance visualizations

---

### 2. SQL Procedures: `model_registry_tools.sql`

**Location:** `agent_tools/src/forecasting_tools/`

**Procedures:**

```sql
-- Wrap VISITOR_FORECASTER
CREATE OR REPLACE PROCEDURE FORECAST_VISITORS_ML(
    FORECAST_DATE DATE,
    DAYS_AHEAD NUMBER
) RETURNS STRING ...

-- Wrap WAIT_TIME_PREDICTOR
CREATE OR REPLACE PROCEDURE PREDICT_WAIT_TIMES_ML(
    TARGET_DATE DATE,
    TARGET_LIFT STRING DEFAULT NULL
) RETURNS STRING ...

-- Wrap STAFFING_OPTIMIZER
CREATE OR REPLACE PROCEDURE OPTIMIZE_STAFFING_ML(
    TARGET_DATE DATE,
    EXPECTED_VISITORS NUMBER DEFAULT NULL
) RETURNS STRING ...

-- Wrap REVENUE_FORECASTER
CREATE OR REPLACE PROCEDURE FORECAST_REVENUE_ML(
    FORECAST_DATE DATE,
    DAYS_AHEAD NUMBER
) RETURNS STRING ...
```

---

### 3. Agent Tool Definitions

**Add to `ski_ops_assistant.yml`:**

```yaml
tools:
  # ... existing Cortex Analyst tools ...

  # ML-Powered Tools
  - name: VisitorForecast
    type: procedure
    description: |
      ML-powered visitor forecasting using Model Registry.
      Predicts daily visitor counts with staffing recommendations.

      USE WHEN: Questions about FUTURE visitor counts, predictions, forecasts
      DO NOT USE: For historical visitor data (use LiftOperationsAnalytics)

    procedure:
      database: SKI_RESORT_DB
      schema: AGENTS
      name: FORECAST_VISITORS_ML

    parameters:
      - name: forecast_date
        type: DATE
        description: Starting date for forecast
      - name: days_ahead
        type: NUMBER
        description: Number of days to forecast (1-14)

  - name: WaitTimePrediction
    type: procedure
    description: |
      ML-powered wait time prediction using Model Registry.
      Predicts lift wait times for operational planning.

      USE WHEN: Questions about FUTURE wait times, predictions
      DO NOT USE: For historical wait times (use LiftOperationsAnalytics)

    procedure:
      database: SKI_RESORT_DB
      schema: AGENTS
      name: PREDICT_WAIT_TIMES_ML

    parameters:
      - name: target_date
        type: DATE
      - name: target_lift
        type: STRING
        required: false

  - name: StaffingOptimizer
    type: procedure
    description: |
      ML-powered staffing recommendations using Model Registry.
      Recommends staff levels by department based on expected visitors.

      USE WHEN: Questions about staffing RECOMMENDATIONS for future dates
      DO NOT USE: For historical staffing data (use StaffingAnalytics)

    procedure:
      database: SKI_RESORT_DB
      schema: AGENTS
      name: OPTIMIZE_STAFFING_ML

    parameters:
      - name: target_date
        type: DATE
      - name: expected_visitors
        type: NUMBER
        required: false
```

---

## Deployment Steps

### Phase 1: Model Development (Notebook)

1. **Create Notebook Environment**
   ```bash
   cd agent_tools/src/forecasting_tools
   mkdir notebooks
   ```

2. **Run Notebook in Snowsight**
   - Upload `train_and_register_models.ipynb`
   - Execute cells to train and register models
   - Verify models in Model Registry

3. **Validate Models**
   ```sql
   SHOW MODELS IN SKI_RESORT_DB.MODELS;
   SELECT * FROM SKI_RESORT_DB.MODELS.VISITOR_FORECASTER.METADATA;
   ```

### Phase 2: Procedure Deployment

1. **Create Wrapper Procedures**
   ```bash
   snow sql -f model_registry_tools.sql -c snowflake_agents
   ```

2. **Test Procedures**
   ```sql
   CALL SKI_RESORT_DB.AGENTS.FORECAST_VISITORS_ML('2024-12-20'::DATE, 7);
   CALL SKI_RESORT_DB.AGENTS.PREDICT_WAIT_TIMES_ML('2024-12-20'::DATE, 'Summit Gondola');
   CALL SKI_RESORT_DB.AGENTS.OPTIMIZE_STAFFING_ML('2024-12-20'::DATE, NULL);
   ```

### Phase 3: Agent Integration

1. **Update Agent Configs**
   - Add ML tools to `ski_ops_assistant.yml`
   - Add ML tools to `resort_executive.yml`

2. **Deploy Agents**
   ```bash
   cd snowflake_agents
   python deploy.py --all --env dev
   ```

3. **Test Agent with ML Tools**
   - "How many visitors can we expect next Saturday?"
   - "Predict wait times for the holiday weekend"
   - "How should we staff for December 25th?"

---

## Model Registry Schema

```sql
-- Create model registry schema
CREATE SCHEMA IF NOT EXISTS SKI_RESORT_DB.MODELS;

-- Models will be registered here:
-- SKI_RESORT_DB.MODELS.VISITOR_FORECASTER
-- SKI_RESORT_DB.MODELS.WAIT_TIME_PREDICTOR
-- SKI_RESORT_DB.MODELS.STAFFING_OPTIMIZER
-- SKI_RESORT_DB.MODELS.REVENUE_FORECASTER
```

---

## Success Criteria

| Metric | Target |
|--------|--------|
| Visitor forecast MAE | < 300 visitors |
| Wait time prediction MAE | < 3 minutes |
| Model inference latency | < 2 seconds |
| Agent successfully calls ML tool | 100% |
| Model version tracking | Working |

---

## Questions Before Implementation

1. **Cortex ML FORECAST vs scikit-learn?**
   - Cortex ML: Built-in, no-code, but less flexible
   - scikit-learn: More control, more features, requires training code

2. **Fallback to Rule-Based?**
   - Should procedures fall back to heuristics if model fails?
   - Recommended: Yes, for reliability

3. **Model Refresh Schedule?**
   - How often to retrain models?
   - Recommended: Weekly during season, monthly off-season

4. **Feature Store?**
   - Should we create a feature table for model inputs?
   - Recommended: Yes, for consistency between training and inference

---

## Timeline Estimate

| Phase | Duration | Dependencies |
|-------|----------|--------------|
| Plan Review | 30 min | This document |
| Notebook Development | 2-3 hours | Historical data |
| Model Training & Registration | 1 hour | Notebook complete |
| Procedure Development | 1 hour | Models registered |
| Agent Integration | 30 min | Procedures deployed |
| Testing & Validation | 1 hour | All components |
| **Total** | **6-7 hours** | |

---

Ready to proceed? Let me know:
1. ✅ Proceed with this plan
2. 🔄 Modify approach (specify changes)
3. ❓ Questions about specific components
