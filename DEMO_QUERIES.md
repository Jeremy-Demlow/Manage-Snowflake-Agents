# Ski Resort Analytics - Demo Queries & Agent Tools

## ✅ All Systems Operational

- **Database**: SKI_RESORT_DB
- **Connection**: agents_example
- **Data**: 4.8M+ transactions (4 years)
- **Status**: Production Ready

---

## 🎯 **Semantic View Queries** (Working & Tested)

### Customer Behavior Analytics

```sql
-- 1. Which customer segments visit most?
SELECT customer_segment, COUNT(*) as total_visits,
       ROUND(AVG(total_lift_rides), 1) as avg_laps
FROM SKI_RESORT_DB.SEMANTIC.SEM_CUSTOMER_BEHAVIOR
GROUP BY customer_segment
ORDER BY total_visits DESC;

/* RESULTS:
local_pass_holder: 127,310 visits (37.5%)
weekend_warrior:    90,674 visits (26.7%)
vacation_family:    60,071 visits (17.7%)
expert_skier:       46,729 visits (13.8%)
*/
```

```sql
-- 2. Pass holder vs day visitor comparison
SELECT is_pass_holder, COUNT(*) as visits,
       ROUND(AVG(total_lift_rides), 1) as avg_laps
FROM SKI_RESORT_DB.SEMANTIC.SEM_CUSTOMER_BEHAVIOR
GROUP BY is_pass_holder;

/* RESULTS:
Pass Holders: 264,713 visits (78%)
Day Visitors:  74,504 visits (22%)
Both average ~10.5 laps per visit
*/
```

```sql
-- 3. Season-over-season trends
SELECT ski_season, is_pass_holder, COUNT(*) as visits,
       ROUND(AVG(hours_on_mountain), 1) as avg_hours
FROM SKI_RESORT_DB.SEMANTIC.SEM_CUSTOMER_BEHAVIOR
GROUP BY ski_season, is_pass_holder
ORDER BY ski_season, is_pass_holder;

/* Shows consistent growth and pass holder loyalty */
```

```sql
-- 4. Geographic distribution - where do visitors come from?
SELECT home_state, COUNT(*) as visits
FROM SKI_RESORT_DB.SEMANTIC.SEM_CUSTOMER_BEHAVIOR
GROUP BY home_state
ORDER BY visits DESC
LIMIT 10;

/* RESULTS:
CO: 173,271 visits (51% - local market)
UT:  36,050 visits (11% - regional)
NM:  34,933 visits (10% - regional)
...destination markets follow
*/
```

```sql
-- 5. Age demographics
SELECT age_group, COUNT(*) as visits,
       COUNT(DISTINCT customer_segment) as segments
FROM SKI_RESORT_DB.SEMANTIC.SEM_CUSTOMER_BEHAVIOR
GROUP BY age_group
ORDER BY visits DESC;

/* RESULTS:
Adult (35-54):       199,863 visits (59%)
Young Adult (18-34): 110,891 visits (33%)
Senior (55+):         18,732 visits (6%)
Child (<18):           9,731 visits (3%)
*/
```

```sql
-- 6. Weekend vs weekday patterns
SELECT ski_season, is_weekend, COUNT(*) as visits
FROM SKI_RESORT_DB.SEMANTIC.SEM_CUSTOMER_BEHAVIOR
GROUP BY ski_season, is_weekend
ORDER BY ski_season;

/* Shows 63% weekday visits (local pass holder driven) */
```

---

## 🤖 **Agent Tool Procedures** (Forecasting & Optimization)

### 1. Visitor Forecasting for Staffing

```sql
-- Predict next 7 days of visitor counts
CALL SKI_RESORT_DB.MARTS_MARTS.PREDICT_DAILY_VISITORS(
    '2024-12-20'::DATE,  -- Start date
    7                     -- Days to forecast
);
```

**Returns JSON with**:
- Predicted visitor counts per day
- Staffing level recommendations (Minimal/Moderate/Full)
- Lift operator needs (8-10, 12-15, or 18+)
- Ticket window requirements (2, 3-4, or 5-6)

**Example Output**:
```json
{
  "forecast_start_date": "2024-12-20",
  "days_forecasted": 7,
  "historical_avg_visitors": 468,
  "recent_7day_avg": 208,
  "forecasts": [
    {
      "date": "2024-12-20",
      "day_of_week": "Friday",
      "predicted_visitors": 269,
      "staffing_level": "Minimal - Core staff only",
      "lift_operators_needed": "8-10 lift operators"
    }
  ]
}
```

### 2. Peak Days Analysis

```sql
-- Identify top 10 busiest days in a season
CALL SKI_RESORT_DB.MARTS_MARTS.GET_PEAK_DAYS_ANALYSIS('2023-2024');
```

**Returns JSON with**:
- Top 10 busiest days
- Holiday flags
- Snow conditions
- Staffing recommendations

**Example Output**:
```json
{
  "season": "2023-2024",
  "max_visitors": 1651,
  "peak_days": [
    {
      "date": "2023-12-30",
      "day_name": "Sat",
      "visitors": 1651,
      "is_holiday": true,
      "snow_condition": "Good"
    }
  ],
  "staffing_recommendation": "FULL_STAFF_REQUIRED"
}
```

### 3. Lift Staffing Optimizer

```sql
-- Optimize lift staffing to keep waits under 15 minutes
CALL SKI_RESORT_DB.MARTS_MARTS.OPTIMIZE_LIFT_STAFFING(
    '2024-01-15'::DATE,  -- Target date
    15                    -- Max acceptable wait time (minutes)
);
```

**Returns JSON with**:
- Lift-by-lift recommendations
- Priority levels (HIGH/MEDIUM/LOW)
- Staff adjustment needed (INCREASE/MAINTAIN/REDUCE)

---

## 📊 **Dimensional Model Direct Queries**

### Fact Tables

```sql
-- Total lift scans with details
SELECT COUNT(*) as total_scans,
       MIN(SCAN_TIMESTAMP) as first_scan,
       MAX(SCAN_TIMESTAMP) as last_scan,
       ROUND(AVG(WAIT_TIME_MINUTES), 1) as avg_wait
FROM SKI_RESORT_DB.MARTS_MARTS.FACT_LIFT_SCANS;

-- Daily visitor summary
SELECT COUNT(*) as total_visit_days,
       COUNT(DISTINCT CUSTOMER_KEY) as unique_customers,
       ROUND(AVG(TOTAL_LIFT_RIDES), 1) as avg_laps_per_visit
FROM SKI_RESORT_DB.MARTS_MARTS.FACT_PASS_USAGE;
```

### Dimension Tables

```sql
-- Date dimension - ski season dates
SELECT * FROM SKI_RESORT_DB.MARTS_MARTS.DIM_DATE
WHERE SKI_SEASON = '2023-2024'
ORDER BY FULL_DATE;

-- Customer segments breakdown
SELECT CUSTOMER_SEGMENT, IS_PASS_HOLDER, COUNT(*) as count
FROM SKI_RESORT_DB.MARTS_MARTS.DIM_CUSTOMER
GROUP BY CUSTOMER_SEGMENT, IS_PASS_HOLDER;

-- Lift infrastructure
SELECT LIFT_NAME, LIFT_TYPE, CAPACITY_PER_HOUR, TERRAIN_TYPE
FROM SKI_RESORT_DB.MARTS_MARTS.DIM_LIFT
ORDER BY CAPACITY_PER_HOUR DESC;
```

---

## 🎿 **Business Intelligence Questions** (All Answerable)

### Customer Questions

1. ✅ "Which customer segments visit most frequently?"
2. ✅ "What's the lifetime value of pass holders vs day visitors?"
3. ✅ "Which age groups are our core customers?"
4. ✅ "Where do our visitors come from geographically?"
5. ✅ "How do vacation families behave differently than locals?"

### Operational Questions

6. ✅ "How many visitors should we expect next weekend?"
7. ✅ "What are the top 10 busiest days we need to staff for?"
8. ✅ "Which lifts need more operators to reduce wait times?"
9. ✅ "What's the optimal staffing for holiday periods?"
10. ✅ "How do weekday patterns differ from weekends?"

### Seasonal Questions

11. ✅ "How did this season compare to last year?"
12. ✅ "Which months are busiest?"
13. ✅ "How does snow condition affect attendance?"
14. ✅ "What's our visitor retention year-over-year?"

---

## 🔧 **Quick Commands**

### Verify Setup
```bash
./scripts/verify_setup.sh
```

### Test Forecasting Tool
```bash
snow sql -q "CALL SKI_RESORT_DB.MARTS_MARTS.PREDICT_DAILY_VISITORS('2024-12-25'::DATE, 7)" --connection agents_example
```

### Query Semantic Views
```bash
snow sql -q "SELECT customer_segment, COUNT(*) FROM SKI_RESORT_DB.SEMANTIC.SEM_CUSTOMER_BEHAVIOR GROUP BY customer_segment" --connection agents_example
```

### Check Data Counts
```bash
snow sql -q "SELECT COUNT(*) FROM SKI_RESORT_DB.MARTS_MARTS.FACT_LIFT_SCANS" --connection agents_example
```

---

## 🚀 **Next Steps for Cortex Analyst**

### 1. Create Agent in Snowsight
- Navigate to: AI & ML > Cortex Analyst
- Create new agent
- Point to: `SKI_RESORT_DB.SEMANTIC.SEM_CUSTOMER_BEHAVIOR`
- Add procedures as tools

### 2. Natural Language Questions to Test
- "How many local pass holders visited last season?"
- "What day of the week is busiest?"
- "Forecast visitors for next weekend"
- "Which states do our visitors come from?"
- "Compare pass holder behavior to day visitors"

### 3. Add Custom YAML Semantic Models
- Create detailed YAML for Cortex Analyst
- Include verified queries
- Add custom instructions
- Deploy to stage for agent consumption

---

## 📈 **Data Highlights**

- **Total Customers**: 8,000
- **Total Visits**: 339,217  
- **Total Lift Scans**: 3,562,442
- **Average Visitor**: 10.5 laps, 6 hours skiing
- **Pass Holder Dominance**: 78% of all visits
- **Local Market**: 51% from Colorado
- **Peak Season**: December-February
- **Best Day**: Dec 30, 2023 (1,651 visitors)

---

**Status: ALL QUERIES TESTED & WORKING** ✅  
**Ready for: Cortex Analyst Agent Deployment** 🤖
