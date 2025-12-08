-- =============================================================================
-- ML Observability Setup for Visitor Forecasting Model
-- =============================================================================
-- Creates the infrastructure for monitoring model performance over time:
-- 1. Prediction log table (stores forecasts + actuals for comparison)
-- 2. Model monitor object (tracks drift and performance)
-- =============================================================================

USE DATABASE SKI_RESORT_DB;
USE SCHEMA MODELS;

-- =============================================================================
-- 1. PREDICTION LOG TABLE
-- Stores every prediction with features, prediction, and eventual actual
-- =============================================================================
CREATE TABLE IF NOT EXISTS ML_PREDICTION_LOG (
    -- Identifiers
    prediction_id VARCHAR DEFAULT UUID_STRING(),
    model_name VARCHAR NOT NULL,
    model_version VARCHAR NOT NULL,

    -- Timestamps
    prediction_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    forecast_date DATE NOT NULL,

    -- Prediction
    predicted_visitors NUMBER,

    -- Actual (filled in later when ground truth available)
    actual_visitors NUMBER,

    -- Key features (for drift detection)
    day_of_week NUMBER,
    month NUMBER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    lag_7 NUMBER,
    lag_14 NUMBER,
    rolling_7_mean NUMBER,

    -- Metadata
    request_source VARCHAR,  -- 'agent', 'api', 'batch'

    PRIMARY KEY (prediction_id)
);

-- Index for efficient lookups
CREATE INDEX IF NOT EXISTS idx_prediction_log_date
    ON ML_PREDICTION_LOG (forecast_date);
CREATE INDEX IF NOT EXISTS idx_prediction_log_model
    ON ML_PREDICTION_LOG (model_name, model_version);

-- =============================================================================
-- 2. BACKFILL ACTUALS PROCEDURE
-- Run daily to fill in actual visitor counts for past predictions
-- =============================================================================
CREATE OR REPLACE PROCEDURE BACKFILL_PREDICTION_ACTUALS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE ML_PREDICTION_LOG p
    SET actual_visitors = (
        SELECT COUNT(DISTINCT CUSTOMER_KEY)
        FROM SKI_RESORT_DB.MARTS.FACT_PASS_USAGE f
        JOIN SKI_RESORT_DB.MARTS.DIM_DATE d ON f.DATE_KEY = d.DATE_KEY
        WHERE d.FULL_DATE = p.forecast_date
    )
    WHERE p.actual_visitors IS NULL
      AND p.forecast_date < CURRENT_DATE();

    RETURN 'Actuals backfilled for ' || SQLROWCOUNT || ' predictions';
END;
$$;

-- =============================================================================
-- 3. MODEL MONITOR (Snowflake ML Observability)
-- Automatically tracks drift and performance metrics
-- =============================================================================

-- First, create a view that the monitor will watch
CREATE OR REPLACE VIEW ML_PREDICTION_MONITOR_SOURCE AS
SELECT
    prediction_timestamp,
    forecast_date,
    predicted_visitors,
    actual_visitors,
    day_of_week,
    month,
    is_weekend::NUMBER as is_weekend,
    lag_7,
    lag_14,
    rolling_7_mean,
    -- Compute error when actuals available
    CASE
        WHEN actual_visitors IS NOT NULL
        THEN ABS(predicted_visitors - actual_visitors)
    END as absolute_error,
    CASE
        WHEN actual_visitors IS NOT NULL AND actual_visitors > 0
        THEN ABS(predicted_visitors - actual_visitors) / actual_visitors * 100
    END as percentage_error
FROM ML_PREDICTION_LOG
WHERE model_name = 'VISITOR_FORECASTER';

-- Create the model monitor (requires the model to be registered first)
-- Note: Run this after the model is in the registry
/*
CREATE MODEL MONITOR IF NOT EXISTS VISITOR_FORECAST_MONITOR
    ON MODEL SKI_RESORT_DB.MODELS.VISITOR_FORECASTER
    VERSION LATEST
    SOURCE TABLE SKI_RESORT_DB.MODELS.ML_PREDICTION_LOG
    TIMESTAMP COLUMN prediction_timestamp
    PREDICTION COLUMN predicted_visitors
    ACTUAL COLUMN actual_visitors
    AGGREGATION WINDOW '1 DAY'
    REFRESH INTERVAL '1 DAY'
    WAREHOUSE COMPUTE_WH;
*/

-- =============================================================================
-- 4. MONITORING QUERIES (for custom dashboards or alerting)
-- =============================================================================

-- Daily model performance summary
CREATE OR REPLACE VIEW ML_DAILY_PERFORMANCE AS
SELECT
    forecast_date,
    model_version,
    COUNT(*) as prediction_count,
    AVG(predicted_visitors) as avg_predicted,
    AVG(actual_visitors) as avg_actual,
    AVG(absolute_error) as mae,
    AVG(percentage_error) as mape,
    STDDEV(percentage_error) as mape_stddev
FROM ML_PREDICTION_MONITOR_SOURCE
WHERE actual_visitors IS NOT NULL
GROUP BY forecast_date, model_version
ORDER BY forecast_date DESC;

-- Feature drift detection (compare recent vs baseline)
CREATE OR REPLACE VIEW ML_FEATURE_DRIFT AS
WITH recent AS (
    SELECT
        AVG(lag_7) as avg_lag_7,
        AVG(lag_14) as avg_lag_14,
        AVG(rolling_7_mean) as avg_rolling_7,
        STDDEV(lag_7) as std_lag_7
    FROM ML_PREDICTION_LOG
    WHERE prediction_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP())
),
baseline AS (
    SELECT
        AVG(lag_7) as avg_lag_7,
        AVG(lag_14) as avg_lag_14,
        AVG(rolling_7_mean) as avg_rolling_7,
        STDDEV(lag_7) as std_lag_7
    FROM ML_PREDICTION_LOG
    WHERE prediction_timestamp >= DATEADD('day', -30, CURRENT_TIMESTAMP())
      AND prediction_timestamp < DATEADD('day', -7, CURRENT_TIMESTAMP())
)
SELECT
    'lag_7' as feature,
    r.avg_lag_7 as recent_mean,
    b.avg_lag_7 as baseline_mean,
    ABS(r.avg_lag_7 - b.avg_lag_7) / NULLIF(b.std_lag_7, 0) as z_score_drift
FROM recent r, baseline b
UNION ALL
SELECT
    'lag_14',
    r.avg_lag_14,
    b.avg_lag_14,
    ABS(r.avg_lag_14 - b.avg_lag_14) / NULLIF(b.std_lag_7, 0)
FROM recent r, baseline b;

-- =============================================================================
-- 5. SCHEDULED TASK FOR BACKFILLING ACTUALS
-- =============================================================================
CREATE OR REPLACE TASK BACKFILL_ACTUALS_DAILY
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 6 * * * America/Los_Angeles'  -- 6am PST daily
AS
    CALL BACKFILL_PREDICTION_ACTUALS();

-- Enable the task
ALTER TASK BACKFILL_ACTUALS_DAILY RESUME;

-- =============================================================================
-- VERIFICATION
-- =============================================================================
SELECT 'ML Monitoring infrastructure created successfully' as status;
SHOW TABLES LIKE 'ML_%' IN SCHEMA SKI_RESORT_DB.MODELS;
SHOW VIEWS LIKE 'ML_%' IN SCHEMA SKI_RESORT_DB.MODELS;
SHOW TASKS LIKE 'BACKFILL%' IN SCHEMA SKI_RESORT_DB.MODELS;
