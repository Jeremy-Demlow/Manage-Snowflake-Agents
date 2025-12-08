# Snowflake Cortex Agents - Ski Resort Analytics

This directory contains the agent configurations for the Ski Resort Analytics platform. Each agent is designed for a specific persona and use case, leveraging Snowflake Cortex semantic views to answer natural language questions.

---

## 🎿 Agent Overview

| Agent | Primary Persona | Tools | Key Value |
|-------|-----------------|-------|-----------|
| **Ski Ops Assistant** | Operations Manager | 3 | Real-time operational decisions |
| **Customer Insights** | Marketing Manager | 3 | Customer acquisition & retention |
| **Resort Executive** | CEO/CFO/COO | 6 | Strategic business intelligence |

---

## 1. Ski Operations Assistant (`ski_ops_assistant.yml`)

### 👤 Target Personas

| Persona | Role | Typical Questions |
|---------|------|-------------------|
| **Lift Supervisor** | Manages daily lift operations | "Which lifts have the longest waits right now?" |
| **Operations Manager** | Oversees mountain-wide operations | "Show me weekend vs weekday wait time patterns" |
| **Staffing Coordinator** | Schedules shifts and coverage | "Which departments are understaffed this week?" |
| **Mountain Safety Lead** | Monitors conditions and safety | "How many high-wind days did we have last month?" |

### 🎯 Value Proposition

> **"Make data-driven operational decisions in seconds, not hours."**

- **Real-time Decisions**: Quickly identify lift bottlenecks, staffing gaps, and weather impacts
- **Pattern Recognition**: Understand hourly, daily, and seasonal operational patterns
- **Resource Optimization**: Allocate staff and resources based on historical demand
- **Weather Correlation**: Link weather conditions to operational metrics

### 🛠️ Tools & Data Access

| Tool | Semantic View | Data Coverage | Key Metrics |
|------|---------------|---------------|-------------|
| **LiftOperationsAnalytics** | `SEM_OPERATIONS` | 3.5M+ lift scans, 4 seasons | Wait times, capacity utilization, scans |
| **StaffingAnalytics** | `SEM_STAFFING_ANALYTICS` | 1,750 shifts, 5 departments | Coverage ratio, understaffed count |
| **WeatherAnalytics** | `SEM_WEATHER_ANALYTICS` | 2,100 observations, 8 zones | Snowfall, powder days, wind |

### 💡 Example Questions

```
✅ In-Scope (Will Answer Well)
- "What are the average wait times by lift?"
- "Which departments are understaffed on weekends?"
- "How many powder days did we have last season?"
- "Show me hourly wait time patterns for Summit Gondola"
- "What's the staffing coverage for Lift Operations?"

❌ Out-of-Scope (Will Decline Gracefully)
- "What's the snow forecast?" → No forecasts, historical only
- "How much revenue did we make?" → Refer to Resort Executive
- "What's the current wait time?" → Not real-time
- "Show me John Smith's schedule" → No individual HR data
```

---

## 2. Customer Insights Assistant (`customer_insights.yml`)

### 👤 Target Personas

| Persona | Role | Typical Questions |
|---------|------|-------------------|
| **Marketing Manager** | Drives campaigns and acquisition | "Which segments respond best to email?" |
| **Sales Director** | Sells season passes and group packages | "What's the ROI of our pass program?" |
| **CRM Analyst** | Manages customer lifecycle | "Which pass holders are at risk of churning?" |
| **VP of Marketing** | Sets marketing strategy | "How has our customer mix changed over time?" |

### 🎯 Value Proposition

> **"Turn 8,000+ customer profiles into actionable marketing insights."**

- **Segment Intelligence**: Understand 7 distinct customer personas and their behaviors
- **Pass Holder ROI**: Quantify the value of your season pass program
- **Churn Prevention**: Identify at-risk customers before they leave
- **Campaign Optimization**: Measure marketing effectiveness by channel and audience
- **Geographic Targeting**: Know where your customers come from

### 🛠️ Tools & Data Access

| Tool | Semantic View | Data Coverage | Key Metrics |
|------|---------------|---------------|-------------|
| **CustomerBehaviorAnalytics** | `SEM_CUSTOMER_BEHAVIOR` | 8,000+ customers, 7 segments | Visit frequency, demographics |
| **PassholderAnalytics** | `SEM_PASSHOLDER_ANALYTICS` | Pass holders, 4 seasons | Utilization, ROI, churn risk |
| **MarketingAnalytics** | `SEM_MARKETING_ANALYTICS` | 17 campaigns, multi-channel | Open rate, conversion, revenue |

### 📊 Customer Segments (7 Personas)

| Segment | % of Visits | Behavior | Value |
|---------|-------------|----------|-------|
| **Local Pass Holder** | 37% | 25-40 visits/season, weekday preference | High lifetime value |
| **Weekend Warrior** | 27% | 12-20 visits, Saturday focus | Reliable revenue |
| **Vacation Family** | 18% | 3-7 days, highest spend ($234/day) | Highest per-visit |
| **Expert Skier** | 14% | Powder chasers, minimal F&B | Weather dependent |
| **Day Tripper** | 3% | 1-4 visits, spontaneous | Acquisition target |
| **Group/Corporate** | <1% | Events, very high spend | Premium segment |
| **Beginner** | <1% | 1-2 visits, rarely return | Conversion opportunity |

### 💡 Example Questions

```
✅ In-Scope (Will Answer Well)
- "What customer segments visit most frequently?"
- "What's the pass holder vs day visitor split?"
- "Which marketing campaigns had the best conversion?"
- "Where do our customers come from?"
- "What's the average visits per pass holder?"

❌ Out-of-Scope (Will Decline Gracefully)
- "Show me customer John Doe's email" → No PII
- "What are the lift wait times?" → Refer to Ski Ops
- "What's our total revenue?" → Limited to campaign attribution
```

---

## 3. Resort Executive Assistant (`resort_executive.yml`)

### 👤 Target Personas

| Persona | Role | Typical Questions |
|---------|------|-------------------|
| **CEO** | Strategic direction | "How is the resort performing overall?" |
| **CFO** | Financial oversight | "What's our revenue breakdown by category?" |
| **COO** | Operational efficiency | "What's our staffing efficiency vs visitor volume?" |
| **General Manager** | Day-to-day leadership | "Compare this season to last season" |
| **Board Member** | Governance and oversight | "What are our key business health indicators?" |

### 🎯 Value Proposition

> **"One agent to answer any question about the resort."**

- **Unified View**: Access ALL resort data through a single interface
- **Cross-Domain Analysis**: Correlate weather, operations, customers, and revenue
- **Executive Summaries**: Get strategic insights, not just raw data
- **Seasonal Comparisons**: Track performance across 4 years of history
- **Decision Support**: Quantified insights for board-level decisions

### 🛠️ Tools & Data Access (Full Resort Coverage)

| Tool | Semantic View | Focus Area |
|------|---------------|------------|
| **DailySummaryKPIs** | `SEM_DAILY_SUMMARY` | Executive KPIs, overall performance |
| **RevenueAnalytics** | `SEM_REVENUE` | Tickets, rentals, F&B breakdown |
| **CustomerAnalytics** | `SEM_CUSTOMER_BEHAVIOR` | Customer segments, demographics |
| **LiftOperations** | `SEM_OPERATIONS` | Lift performance, wait times |
| **StaffingAnalytics** | `SEM_STAFFING_ANALYTICS` | Labor efficiency, coverage |
| **WeatherAnalytics** | `SEM_WEATHER_ANALYTICS` | Weather patterns, correlations |

### 💡 Example Questions

```
✅ In-Scope (Will Answer Well)
- "Give me a complete resort performance summary"
- "What's our revenue breakdown by category?"
- "Compare this season to last season"
- "How does weather impact our daily revenue?"
- "What's our customer segment profitability?"
- "Which days of the week drive the most revenue?"

❌ Out-of-Scope (Will Decline Gracefully)
- "What will revenue be next month?" → No predictions
- "What's our profit margin?" → No cost/P&L data
- "Show me the org chart" → No HR data
```

---

## 🔑 Key Differentiators

### Why Three Agents Instead of One?

| Factor | Single Agent | Multiple Specialized Agents |
|--------|--------------|----------------------------|
| **Response Quality** | Generic, may miss nuances | Domain-expert responses |
| **Tool Selection** | May pick wrong tool | Clear tool boundaries |
| **Instructions** | Bloated, conflicting | Focused, role-specific |
| **User Experience** | Overwhelming options | Persona-aligned |
| **Maintainability** | Hard to update | Modular, easy to evolve |

### Agent Hierarchy

```
┌─────────────────────────────────────────────────────────┐
│              RESORT EXECUTIVE ASSISTANT                 │
│         (CEO, CFO, COO, Board Members)                  │
│                  6 Tools - Full Access                  │
└─────────────────────────────────────────────────────────┘
                          │
          ┌───────────────┴───────────────┐
          ▼                               ▼
┌─────────────────────┐     ┌─────────────────────────┐
│  SKI OPS ASSISTANT  │     │ CUSTOMER INSIGHTS AGENT │
│  (Operations Team)  │     │    (Marketing Team)     │
│   3 Tools - Ops     │     │   3 Tools - Customer    │
└─────────────────────┘     └─────────────────────────┘
```

---

## 📁 Directory Structure

```
agents/
├── README.md                          # This file
├── ski_ops_assistant.yml              # Operations agent config
├── customer_insights.yml              # Marketing agent config  
├── resort_executive.yml               # Executive agent config
├── environments/                      # Environment-specific settings
│   ├── dev.yml                       # Development (SKI_RESORT_DB.AGENTS.*_DEV)
│   ├── staging.yml                   # Staging (SKI_RESORT_DB.AGENTS.*_STAGING)
│   └── prod.yml                      # Production (SKI_RESORT_DB.AGENTS.*)
└── tests/                            # Agent validation
    ├── golden_questions.yml          # Quick smoke test questions
    └── golden_qa_dataset.yml         # Full Q&A with ground truth (TruLens)
```

---

## 🚀 Quick Start

```bash
# Deploy single agent
python deploy.py --agent ski_ops_assistant --env dev

# Deploy all agents
python deploy.py --all --env dev

# Run TruLens GPA evaluation
python trulens_eval.py --agent ski_ops_assistant --env dev

# View in Snowsight
# Navigate to: AI & ML → Agents → [Agent Name]
```

---

## 📊 Evaluation Results (GPA Framework)

The agents are evaluated using the **Goal-Plan-Action (GPA)** framework:

| Metric | Description | Target |
|--------|-------------|--------|
| **Goal** | Did the agent answer correctly? | ≥ 90% |
| **Plan** | Did it select the right tool? | 100% |
| **Action** | Is the generated SQL semantically correct? | ≥ 70% |

**Latest Results (ski_ops_assistant):**
- Goal: **100%** ✅
- Plan: **100%** ✅  
- Action: **70%** ⚠️
- **Overall GPA: 90%**

---

## 🔗 Related Documentation

- [Main README](../README.md) - Deployment and evaluation guide
- [Snowflake Cortex Agents](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents)
- [TruLens GPA Framework](https://www.trulens.org/trulens/getting_started/core_concepts/agents/)
- [Snowflake AI Observability](https://docs.snowflake.com/en/user-guide/ai-ml/ai-observability)
