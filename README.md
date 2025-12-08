# Ski Resort Analytics Platform

A production-ready ski resort analytics platform using dbt dimensional modeling (Kimball approach), Snowflake Cortex Agents, and ML-powered visitor forecasting.

## Features

- **Kimball Dimensional Model**: 6 dimensions + 6 fact tables with 8M+ records
- **5 Years of Realistic Data**: Nov 2020 - Dec 2025 with sophisticated customer behavior
- **7 Customer Personas**: Local pass holders to vacation families with unique patterns
- **11 Semantic Views**: Ready for Cortex Analyst natural language queries
- **3 Cortex Agents**: Operations, Customer Insights, and Executive dashboards
- **ML Forecasting**: XGBoost visitor prediction with Model Registry integration
- **Scheduled Alerts**: Subscribe to daily/weekly email reports with AI-powered insights

## Quick Start

```bash
# 1. Activate environment
conda activate snowflake-agents

# 2. Verify connection
snow sql -q "SELECT CURRENT_USER(), CURRENT_ROLE()" --connection snowflake_agents

# 3. Run verification
./scripts/verify_setup.sh

# 4. Query data
snow sql -q "SELECT * FROM SKI_RESORT_DB.MARTS.SEM_CUSTOMER_BEHAVIOR LIMIT 10" --connection snowflake_agents
```

## Project Structure

```
Manage-Snowflake-Agents/
├── dbt_ski_resort/              # dbt project
│   ├── models/
│   │   ├── staging/             # 25 staging views
│   │   └── marts/
│   │       ├── dimensions/      # 6 dimension tables
│   │       ├── facts/           # 6 fact tables (incremental)
│   │       └── semantic/        # 11 semantic views
│   └── seeds/                   # Reference data CSVs
│
├── data_generation/             # Data generation
│   ├── shared.py               # Constants & utilities (single source of truth)
│   ├── generate_complete_ski_data.py  # Full 5-year generation
│   └── generate_daily_increment.py    # Daily incremental (idempotent)
│
├── snowflake_agents/            # Cortex Agents
│   ├── agents/                  # Agent YAML configs
│   │   ├── ski_ops_assistant.yml
│   │   ├── customer_insights.yml
│   │   └── resort_executive.yml
│   ├── deploy.py               # Agent deployment
│   └── trulens_eval.py         # GPA evaluation
│
├── agent_tools/                 # ML & Tools
│   └── src/forecasting_tools/  # Visitor forecasting
│       ├── config/             # YAML-driven feature config
│       ├── models/             # Feature engineering
│       ├── notebooks/          # Training & prediction
│       └── scripts/            # Batch forecasts
│
└── sql/                        # Setup scripts
    ├── setup_ski_resort_db.sql
    └── create_raw_tables.sql
```

## Data Model

### Schemas

| Schema | Purpose |
|--------|---------|
| `RAW` | Raw transactional data (20+ tables) |
| `STAGING` | Type-safe staging views |
| `MARTS` | Dimensional model + semantic views |
| `MODELS` | ML model registry |
| `AGENTS` | Deployed Cortex Agents |

### Dimensions (6)

| Dimension | Rows | Description |
|-----------|------|-------------|
| `dim_date` | 2,500+ | Ski seasons, holidays, weather flags |
| `dim_customer` | 8,000 | 7 personas with behavioral patterns |
| `dim_lift` | 18 | Capacity, terrain, difficulty |
| `dim_location` | 21 | Rental, F&B, ticket locations |
| `dim_product` | 31 | Equipment and F&B items |
| `dim_ticket_type` | 18 | Passes and ticket types |

### Facts (6 - Incremental)

| Fact | Records | Grain |
|------|---------|-------|
| `fact_lift_scans` | 8M+ | Every lift scan with wait times |
| `fact_pass_usage` | 520K+ | Daily customer visits |
| `fact_ticket_sales` | 180K+ | Ticket purchases |
| `fact_food_beverage` | 2M+ | F&B transactions |
| `fact_rentals` | 600K+ | Equipment rentals |
| `fact_weather` | 7K+ | Daily weather by zone |

### Semantic Views (11)

| View | Use Case |
|------|----------|
| `sem_operations` | Lift utilization, wait times |
| `sem_customer_behavior` | Visit patterns, segments |
| `sem_passholder_analytics` | Pass holder ROI |
| `sem_marketing_analytics` | Campaign performance |
| `sem_revenue` | Revenue breakdown |
| `sem_staffing` | Department coverage |
| `sem_weather` | Conditions analysis |
| `sem_lessons` | Ski school metrics |
| `sem_incidents` | Safety analytics |
| `sem_feedback` | Customer satisfaction |
| `sem_daily_summary` | Executive KPIs |

## Cortex Agents

| Agent | Persona | Tools |
|-------|---------|-------|
| **Ski Ops Assistant** | Lift supervisors, Operations | 3 semantic views |
| **Customer Insights** | Marketing, Sales | 4 semantic views |
| **Resort Executive** | CEO, CFO, Board | 6 semantic views |

### Deploy & Test

```bash
cd snowflake_agents

# Deploy all agents
python deploy.py --all --env dev

# Run GPA evaluation
python trulens_eval.py --agent ski_ops_assistant --env dev
```

## ML Forecasting

Predict daily visitor counts for staffing decisions:

```bash
cd agent_tools/src/forecasting_tools

# Train model (in Snowflake Notebooks)
# notebooks/train_visitor_forecast.ipynb

# Generate batch forecast
python scripts/run_batch_forecast.py
```

## Scheduled Alerts

Subscribe to recurring AI-powered email reports:

```
User: "Subscribe me to daily updates on lift wait times"
Agent: ✅ "You'll receive daily updates at 8am MST!"
```

**Setup:**
```bash
cd agent_tools/src/scheduled_alerts

# 1. Create database objects
snow sql -f schedule_analysis.sql --connection snowflake_agents

# 2. Deploy Snowflake Task (uses Python DAG API)
python deploy_alert_task.py
```

**Agent Commands:**
- "Subscribe me to daily revenue reports"
- "What alerts am I subscribed to?"
- "Unsubscribe me from the weekly updates"

## Daily Operations

### Incremental Data Load

```bash
cd data_generation

# Generate today's data (idempotent - won't duplicate)
python generate_daily_increment.py

# Generate specific date range
python generate_daily_increment.py --date 2025-12-04 --days 7

# Force regeneration
python generate_daily_increment.py --date 2025-12-03 --force
```

### Refresh dbt Models

```bash
cd dbt_ski_resort

# Incremental refresh
dbt run --select tag:fact --profiles-dir .

# Full refresh
dbt run --full-refresh --profiles-dir .

# Run tests
dbt test --profiles-dir .
```

## Customer Personas

| Persona | % | Visits/Season | Behavior |
|---------|---|---------------|----------|
| Local Pass Holder | 15% | 25-40 | Early arrivals, favorites runs |
| Weekend Warrior | 25% | 12-20 | Saturday peaks |
| Vacation Family | 30% | 3-7 consecutive | High spend, lessons |
| Day Tripper | 20% | 1-4 | Weather dependent |
| Expert Skier | 5% | 15-25 | Powder chaser |
| Corporate Group | 3% | 1-2 events | Very high spend |
| Beginner | 2% | 1-2 ever | Full rentals |

## Demo Questions

Ask the agents:

1. "What are average wait times by lift this season?"
2. "Which customer segments have highest lifetime value?"
3. "How does powder day affect attendance?"
4. "Which departments are understaffed this week?"
5. "What's our season pass renewal rate?"
6. "Give me a resort performance summary"

## Technology Stack

- **Snowflake**: Data warehouse, Model Registry, Cortex Agents
- **dbt**: Data transformation (1.11+)
- **Python**: Data generation, ML (3.11+)
- **XGBoost**: Visitor forecasting
- **TruLens**: Agent evaluation

## License

MIT License - see LICENSE file
