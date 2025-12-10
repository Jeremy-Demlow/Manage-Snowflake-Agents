# Demo Jam Script: Snowflake Intelligence
## For Technical Leaders (VP Engineering, CTO, CIO)

> **Total Time:** 5 minutes  
> **Theme:** "From Question to Production in Minutes"  
> **Hook:** Show the journey from ad-hoc question → scheduled intelligence → monitored ML → production-grade AI

---

## 🎬 Opening Hook (30 seconds)

**Say This:**
> "Imagine your CEO asks 'How's the business doing?' at 4:47 PM on a Friday.
>
> Today, that triggers a fire drill - someone pulls data, builds a dashboard, writes an email.
>
> With Snowflake Intelligence, that same question becomes a 30-second conversation that automatically repeats every week. Let me show you."

---

## Scene 1: The Executive Question (60 seconds)

**Purpose:** Show natural language → SQL → insight (Cortex Analyst)

### Question 1: Executive Summary
```
Give me a complete performance summary for this week -
revenue trends, top customer segments, and any operational concerns.
```

**While it's running, say:**
> "Notice I'm not writing SQL. The agent understands our semantic model - it knows what 'revenue' means in our context, which tables to join, how we define customer segments."

**After results, say:**
> "This hit 3 different semantic views, joined data correctly, and formatted it for an executive. But here's where it gets interesting..."

---

## Scene 2: Cross-Data Intelligence (60 seconds)

**Purpose:** Show Cortex Search + Analyst working together (NEW capability!)

### Question 2: Structured + Unstructured
```
Our Q4 strategic plan set revenue targets. Did we hit them?
Show me actual vs plan.
```

**While it's running, say:**
> "Watch what happens here - the agent searches our unstructured documents to find the Q4 plan targets, then queries the structured data to compare actuals. Two different data types, one intelligent answer."

**After results, highlight:**
> "This is the unlock - your executives don't care if data lives in a PDF, a Confluence page, or a Snowflake table. They just want answers."

---

## Scene 3: Predictive Intelligence (45 seconds)

**Purpose:** Show ML integration (Cortex ML Functions)

### Question 3: ML Forecast
```
We have holiday staffing decisions to make.
What visitor volumes should we expect Dec 20-31?
```

**While it's running, say:**
> "This calls a real XGBoost model trained on 4 years of historical data - seasonal patterns, weather impact, day-of-week effects. Your data science team built it, we registered it in Snowflake's Model Registry."

**After results, say:**
> "The agent doesn't just query data - it orchestrates ML inference and explains confidence levels. Same natural language interface."

---

## Scene 4: From Insight to Action (45 seconds)

**Purpose:** Show email + scheduling (Production workflow)

### Question 4: Operationalize It
```
This is exactly what I need for our Monday leadership meeting.
Email me this report and set up a weekly subscription.
```

**While it's running, say:**
> "This is the production moment - we're not just answering questions, we're building automated intelligence pipelines. The agent calls a procedure that creates a Snowflake Task."

**After results, say:**
> "Every Monday at 7am, this exact analysis runs, formats as HTML, and lands in their inbox. No dashboard to check, no report to pull. Intelligence delivered."

---

## Scene 5: Production-Grade AI (60 seconds)

**Purpose:** Show monitoring, evaluation, governance

**[Switch to Terminal / Snowsight - show actual metrics]**

### Show the TruLens GPA Dashboard

**Say:**
> "Now here's what makes this enterprise-ready. We're not just deploying AI and hoping it works."

**Pull up the evaluation metrics and say:**
> "Every response is graded on three dimensions:
> - **Goal**: Did it understand the question?
> - **Plan**: Did it pick the right tools?
> - **Action**: Did it execute correctly?
>
> We run these against a golden dataset of 100+ questions. When someone changes a prompt or adds a tool, we know immediately if it helped or hurt."

### Show ML Observability (if time)

**Quick show of:**
```sql
SELECT * FROM ML_DAILY_PERFORMANCE ORDER BY date DESC LIMIT 7;
```

**Say:**
> "Same for ML - we're logging every prediction, comparing to actuals, tracking drift. This is MLOps built into the platform."

---

## 🎯 Closing (30 seconds)

**Say:**
> "Let's recap what we just saw in 5 minutes:
>
> 1. Natural language to SQL across complex data models
> 2. Unified search across structured AND unstructured data
> 3. ML inference with a single question
> 4. Automated delivery via email and schedules
> 5. Production monitoring and evaluation
>
> This isn't a demo environment - this is how your team ships AI to production. The same governance, the same security, the same Snowflake you already trust."

---

## 💡 Backup Questions (If They Want More)

### For "How does security work?"
```
What customer segments can see which reports?
Show me the data access patterns.
```
*Pivots to row-level security, governance*

### For "What about hallucinations?"
```
What's the P&L for our Mars colony operations?
```
*Shows graceful "I don't have data for that" response - grounded in actual data*

### For "Can it handle complexity?"
```
Show me customers whose visit frequency dropped more than 30% compared
to last year, who haven't responded to any marketing campaigns,
and have a lifetime value over $500. What's the churn risk?
```
*Shows multi-step reasoning with complex business logic*

### For "How do we customize this?"
```
Show me how to add a new semantic view for a custom metric.
```
*Pivots to semantic model YAML, shows how teams extend*

---

## 🛠️ Pre-Demo Checklist

- [ ] Agent is deployed: `RESORT_EXECUTIVE_DEV`
- [ ] Documents are loaded: `SKI_RESORT_DB.DOCS.RESORT_DOCS_SEARCH`
- [ ] Recent data exists (run data generator if needed)
- [ ] Email is configured (or mock the send)
- [ ] TruLens results are available to show
- [ ] Terminal open with `ML_DAILY_PERFORMANCE` query ready

---

## 🎭 Persona Hooks by Audience

### For CTO:
> "Your team is probably building RAG pipelines with LangChain, managing vector stores, building evaluation frameworks. This is all of that, native to Snowflake, with your existing governance."

### For VP Engineering:
> "How long would it take your team to build this? The semantic layer, the agent orchestration, the ML serving, the email delivery, the monitoring? This is weeks of work, already done."

### For CIO:
> "Every AI project I see has the same problem - shadow IT, ungoverned data access, no audit trail. This runs inside Snowflake. Same RBAC, same audit logs, same compliance posture."

---

## 📊 Architecture Slide (For Deep Dives)

```
┌─────────────────────────────────────────────────────────────┐
│                    SNOWFLAKE INTELLIGENCE                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │   Cortex    │    │   Cortex    │    │   Cortex    │     │
│  │   Analyst   │    │   Search    │    │     ML      │     │
│  │ (Text→SQL)  │    │ (Documents) │    │ (Forecast)  │     │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘     │
│         │                  │                  │             │
│         └──────────────────┼──────────────────┘             │
│                            │                                │
│                    ┌───────▼───────┐                        │
│                    │ Cortex Agent  │                        │
│                    │  Orchestrator │                        │
│                    └───────┬───────┘                        │
│                            │                                │
│  ┌────────────────────────┬┴───────────────────────┐       │
│  │                        │                        │       │
│  ▼                        ▼                        ▼       │
│ Email               Scheduled Tasks          Monitoring    │
│ Delivery            (Automated Intel)        (TruLens)     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
              ┌──────────────────────────┐
              │   YOUR EXISTING DATA     │
              │  Same Security • Same    │
              │  Governance • Same Cost  │
              └──────────────────────────┘
```

---

*Built for Demo Jam - Snowflake Intelligence*
