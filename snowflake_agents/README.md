# Snowflake Intelligence Agents

Deploy and manage Cortex Agents for Snowflake Intelligence.

## 🎿 Agents Overview

This project includes **3 specialized agents** for ski resort analytics:

| Agent | Persona | Tools | Value Add |
|-------|---------|-------|-----------|
| **Ski Ops Assistant** | Lift Supervisors, Operations Managers | 3 | Real-time operational decisions on lift wait times, staffing gaps, weather impact |
| **Customer Insights** | Marketing Managers, Sales Directors | 3 | Customer segmentation, pass holder ROI, campaign effectiveness |
| **Resort Executive** | CEO, CFO, COO, Board | 6 | Unified view across all data for strategic decisions |

See [agents/README.md](agents/README.md) for detailed persona mapping and example questions.

---

## Quick Start

```bash
# 1. Activate conda environment
conda activate snowflake_agents

# 2. Install dependencies
pip install -r requirements.txt

# 3. Verify connection and permissions
python setup.py

# 4. Deploy all agents to dev
python deploy.py --all --env dev

# 5. Run GPA evaluation
python trulens_eval.py --agent ski_ops_assistant --env dev
```

---

## 📁 File Reference

### Core Scripts (Required)

| File | Purpose | When to Use |
|------|---------|-------------|
| `deploy.py` | Deploy agents to Snowflake | Creating/updating agents in any environment |
| `trulens_eval.py` | GPA evaluation with TruLens | Full evaluation with Goal-Plan-Action scores |
| `snowflake_connection.py` | Shared connection utilities | Used by all other scripts |

### Setup Scripts (One-time)

| File | Purpose | When to Use |
|------|---------|-------------|
| `setup.py` | Verify connection, create schemas | Initial setup or troubleshooting |
| `setup_permissions.sql` | SQL permissions (run as ACCOUNTADMIN) | Initial setup or new role configuration |
| `setup_golden_dataset.py` | Create GOLDEN_QA_DATASET table | Before running TruLens evaluation |

### Configuration

| File | Purpose |
|------|---------|
| `requirements.txt` | Python dependencies |
| `agents/*.yml` | Agent configurations (see below) |
| `agents/environments/*.yml` | Environment-specific settings (dev/staging/prod) |
| `agents/tests/*.yml` | Test questions and golden Q&A datasets |

### Optional/Generated

| File | Purpose | Status |
|------|---------|--------|
| `evaluate.py` | Quick verification without TruLens | **Optional** - use `trulens_eval.py` instead |
| `generated/*.sql` | Generated deployment SQL | **Auto-generated** - kept for audit trail |

---

## Directory Structure

```
snowflake_agents/
├── README.md                 # This file
├── deploy.py                 # Main deployment script
├── trulens_eval.py           # TruLens GPA evaluation
├── evaluate.py               # Basic verification (optional)
├── setup_golden_dataset.py   # Setup golden Q&A table
├── setup.py                  # Setup and verification script
├── setup_permissions.sql     # SQL permissions (run as ACCOUNTADMIN)
├── requirements.txt          # Python dependencies
├── snowflake_connection.py   # Shared connection utilities
├── agents/                   # Agent configurations
│   ├── README.md            # Agent descriptions & personas
│   ├── ski_ops_assistant.yml
│   ├── customer_insights.yml
│   ├── resort_executive.yml
│   ├── environments/
│   │   ├── dev.yml
│   │   ├── staging.yml
│   │   └── prod.yml
│   └── tests/
│       ├── golden_questions.yml      # Quick test questions
│       └── golden_qa_dataset.yml     # Q&A with ground truth
└── generated/               # Generated SQL (audit trail)
```

---

## Deployment

### Deploy Single Agent
```bash
python deploy.py --agent ski_ops_assistant --env dev
```

### Deploy All Agents
```bash
python deploy.py --all --env dev
```

### Dry Run (Generate SQL only)
```bash
python deploy.py --agent resort_executive --env prod --dry-run
```

### List Available Agents
```bash
python deploy.py --list
```

## Deployment Architecture

Agents are deployed to **your data database** (not Snowflake Intelligence directly):

```
SKI_RESORT_DB.AGENTS.SKI_OPS_ASSISTANT_DEV    # Dev agent
SKI_RESORT_DB.AGENTS.SKI_OPS_ASSISTANT        # Prod agent
```

Then **added to Snowflake Intelligence** for UI visibility:

```sql
ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT
  ADD AGENT SKI_RESORT_DB.AGENTS.SKI_OPS_ASSISTANT;
```

This keeps Snowflake Intelligence clean and keeps agents with your data!

| Environment | Agent Location | Suffix |
|------------|----------------|--------|
| `dev` | SKI_RESORT_DB.AGENTS | `_DEV` |
| `staging` | SKI_RESORT_DB.AGENTS | `_STAGING` |
| `prod` | SKI_RESORT_DB.AGENTS | (none) |

---

## Permissions Setup

Run the permissions SQL as ACCOUNTADMIN:
```bash
snow sql -f setup_permissions.sql -c snowflake_agents
```

Required permissions:
- `CORTEX_USER` database role
- `AI_OBSERVABILITY_EVENTS_LOOKUP` application role
- `CREATE AGENT` on target schemas
- Access to semantic views in `SKI_RESORT_DB.SEMANTIC`

---

## AI Observability

Agents are automatically monitored via Snowflake AI Observability:

```sql
-- View agent traces
SELECT * FROM TABLE(
  SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS(
    'SNOWFLAKE_INTELLIGENCE_DEV',
    'AGENTS',
    'SKI_OPS_ASSISTANT',
    'CORTEX AGENT'
  )
);

-- View user feedback
SELECT * FROM TABLE(
  SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS(...)
) WHERE RECORD:name = 'CORTEX_AGENT_FEEDBACK';
```

In Snowsight: **AI & ML → Agents → [Agent Name] → Monitoring**

---

## Testing & Evaluation

### TruLens GPA Evaluation (Recommended)

Uses the **Goal-Plan-Action (GPA)** framework for comprehensive agent evaluation:

| Phase | What It Measures | How |
|-------|-----------------|-----|
| **Goal** | Answer quality & relevance | LLM judge compares to golden answer |
| **Plan** | Tool selection accuracy | Checks if correct tool was used |
| **Action** | SQL correctness | Semantic + result comparison |

**Run Evaluation:**
```bash
# Setup golden dataset (once)
python setup_golden_dataset.py --env dev

# Run GPA evaluation
python trulens_eval.py --agent ski_ops_assistant --env dev

# View results in Snowsight: AI & ML → Evaluations
```

**Sample Output:**
```
===========================================================================
🎯 GPA EVALUATION: ski_ops_assistant (dev)
===========================================================================

📊 GPA Scores:
   Goal (Answer Quality):  100%
   Plan (Tool Selection):  100%
   Action (SQL Semantic):  70%
   ────────────────────────────────
   Overall GPA:            90%
```

### Quick Verification (Optional)

For quick smoke tests without TruLens dependencies:

```bash
python evaluate.py --agent ski_ops_assistant --env dev --verify
```

---

## 🎿 Agent Details

### Ski Operations Assistant

**Target Users:** Lift supervisors, operations managers, staffing coordinators

**Tools:**
- `LiftOperationsAnalytics` - Wait times, lift scans, capacity
- `StaffingAnalytics` - Coverage ratios, understaffing
- `WeatherAnalytics` - Snowfall, powder days, wind

**Key Questions:**
- "What are the average wait times by lift?"
- "Which departments are understaffed?"
- "How many powder days did we have?"

---

### Customer Insights Assistant

**Target Users:** Marketing managers, sales directors, CRM analysts

**Tools:**
- `CustomerBehaviorAnalytics` - 8,000+ customers, 7 segments
- `PassholderAnalytics` - Pass holder ROI and utilization
- `MarketingAnalytics` - Campaign performance, conversion rates

**Key Questions:**
- "What customer segments visit most frequently?"
- "What's the pass holder vs day visitor split?"
- "Which campaigns have the best conversion rates?"

---

### Resort Executive Assistant

**Target Users:** CEO, CFO, COO, General Manager, Board members

**Tools:** All 6 semantic views for complete resort coverage

**Key Questions:**
- "Give me a resort performance summary"
- "What's our revenue breakdown by category?"
- "How does weather impact attendance?"

---

## Evaluation Metrics

| Metric | Description | Target |
|--------|-------------|--------|
| **Goal Score** | Is the answer correct and relevant? | ≥ 90% |
| **Plan Score** | Was the right tool selected? | 100% |
| **Action Score** | Is the SQL semantically correct? | ≥ 70% |
| **Overall GPA** | Weighted average | ≥ 85% |

---

## Troubleshooting

### Agent Not Responding
```bash
# Verify agent exists
python deploy.py --list

# Re-deploy
python deploy.py --agent ski_ops_assistant --env dev
```

### Permission Errors
```bash
# Re-run permissions (as ACCOUNTADMIN)
snow sql -f setup_permissions.sql -c snowflake_agents
```

### TruLens Evaluation Errors
```bash
# Ensure conda environment
conda activate snowflake_agents

# Install/update TruLens
pip install -U trulens-core trulens-connectors-snowflake trulens-providers-cortex
```
