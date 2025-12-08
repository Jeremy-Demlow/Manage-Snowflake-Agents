"""
Ski Resort Forecasting Tools
ML-powered visitor prediction for Snowflake Cortex Agents

Components:
- forecast_handler.py: Snowpark procedure handler
- models/feature_engineer.py: YAML-driven feature engineering
- models/config.py: Configuration management
- config/: YAML configuration files

Note: No barrel exports - Snowflake procedures use explicit handler paths
e.g., forecasting_tools.forecast_handler.forecast_visitors
"""
