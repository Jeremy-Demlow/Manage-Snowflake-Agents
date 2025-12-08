# Ski Resort Analytics - Setup Complete! ✓

## Project Summary

Successfully built a production-ready ski resort analytics platform with:

### ✅ Completed Components

1. **Database & Schemas**
   - `SKI_RESORT_DB` database created
   - RAW, STAGING, MARTS, SEMANTIC schemas configured

2. **Data Generated (4 years: Nov 2020 - Apr 2024)**
   - 8,000 customers across 7 realistic personas
   - 3,562,442 lift scans with wait times
   - 339,217 pass usage records
   - 74,504 ticket sales
   - 203,585 rental transactions
   - 677,837 F&B transactions

3. **DBT Dimensional Model (Kimball)**
   - 6 Dimension tables
   - 2 Fact tables (incremental)
   - 7 Staging views
   - All verified via Snowflake CLI

4. **Semantic Views for Analytics**
   - `sem_customer_behavior`: Customer segments and visit patterns
   - `sem_operations`: Lift operations (ready to query)
   - Ready for Snowflake Cortex Analyst integration

---

## Quick Test Queries

### Customer Insights

```sql
-- Which customer segments visit most?
SELECT customer_segment, COUNT(*) as total_visits, ROUND(AVG(total_lift_rides), 1) as avg_laps
FROM SKI_RESORT_DB.SEMANTIC.SEM_CUSTOMER_BEHAVIOR
GROUP BY customer_segment
ORDER BY total_visits DESC;

-- Pass holder vs day visitor behavior
SELECT is_pass_holder, COUNT(*) as visits, ROUND(AVG(hours_on_mountain), 1) as avg_hours
FROM SKI_RESORT_DB.SEMANTIC.SEM_CUSTOMER_BEHAVIOR
GROUP BY is_pass_holder;

-- Season-over-season trends
SELECT ski_season, is_pass_holder, COUNT(*) as visits
FROM SKI_RESORT_DB.SEMANTIC.SEM_CUSTOMER_BEHAVIOR  
GROUP BY ski_season, is_pass_holder
ORDER BY ski_season;

-- Geographic distribution
SELECT home_state, COUNT(*) as visits
FROM SKI_RESORT_DB.SEMANTIC.SEM_CUSTOMER_BEHAVIOR
GROUP BY home_state
ORDER BY visits DESC
LIMIT 10;

-- Age demographics
SELECT age_group, COUNT(*) as visits, COUNT(DISTINCT customer_segment) as segments
FROM SKI_RESORT_DB.SEMANTIC.SEM_CUSTOMER_BEHAVIOR
GROUP BY age_group
ORDER BY visits DESC;
```

### Dimensional Model Queries

```sql
-- Date dimension
SELECT * FROM SKI_RESORT_DB.MARTS_MARTS.DIM_DATE LIMIT 10;

-- Customer dimension
SELECT * FROM SKI_RESORT_DB.MARTS_MARTS.DIM_CUSTOMER LIMIT 10;

-- Lift infrastructure
SELECT * FROM SKI_RESORT_DB.MARTS_MARTS.DIM_LIFT;

-- Fact tables
SELECT COUNT(*) FROM SKI_RESORT_DB.MARTS_MARTS.FACT_LIFT_SCANS;
SELECT COUNT(*) FROM SKI_RESORT_DB.MARTS_MARTS.FACT_PASS_USAGE;
```

---

## Key Statistics

### Customer Distribution
- **Local Pass Holders**: 127,310 visits (37.5%)
- **Weekend Warriors**: 90,674 visits (26.7%)
- **Vacation Families**: 60,071 visits (17.7%)
- **Expert Skiers**: 46,729 visits (13.8%)
- **Day Trippers**: 11,596 visits (3.4%)

### Pass Holder Insights
- 78% of visits from pass holders (264,713 visits)
- 22% from day visitors (74,504 visits)
- Pass holders average 10.5 laps per visit
- Day visitors also average 10.5 laps (similar activity level)

### Geographic Insights
- 51% of visits from Colorado residents
- 31% from nearby states (UT, NM, WY)
- 18% from distant states (destination visitors)

### Age Demographics
- Adults (35-54): 59% of visits
- Young Adults (18-34): 33% of visits
- Seniors (55+): 6% of visits
- Children (<18): 3% of visits

---

## Next Steps

### 1. Create Cortex Analyst Semantic Model (YAML)
```bash
# Create comprehensive YAML semantic models for Cortex Analyst
# These enable natural language queries like:
# "What is the average wait time on weekends?"
# "Which customer segments have the highest visit frequency?"
```

### 2. Add Revenue Analytics
```sql
-- Create views joining ticket_sales, rentals, F&B for complete revenue picture
-- Enable queries like:
# "What is our total revenue by source?"
# "Which customer segment spends the most per visit?"
```

### 3. Deploy Cortex Analyst Agent
- Upload semantic model YAML to Snowflake stage
- Create Cortex Analyst agent
- Test natural language queries

### 4. Build Dashboards
- Streamlit dashboard using semantic views
- Real-time operations monitoring
- Customer segmentation analysis

---

## Verification Commands

```bash
# Run complete verification
./scripts/verify_setup.sh

# Check specific tables
snow sql -q "SELECT COUNT(*) FROM SKI_RESORT_DB.RAW.CUSTOMERS" --connection agents_example
snow sql -q "SELECT COUNT(*) FROM SKI_RESORT_DB.MARTS_MARTS.FACT_LIFT_SCANS" --connection agents_example

# Test semantic views
snow sql -q "SELECT * FROM SKI_RESORT_DB.SEMANTIC.SEM_CUSTOMER_BEHAVIOR LIMIT 10" --connection agents_example
```

---

## DBT Commands

```bash
cd dbt_ski_resort

# Re-run models
conda run -n snowflake-agents dbt run --profiles-dir .

# Run tests
conda run -n snowflake-agents dbt test --profiles-dir .

# View documentation
conda run -n snowflake-agents dbt docs generate --profiles-dir .
conda run -n snowflake-agents dbt docs serve --profiles-dir .
```

---

## Project Highlights

✅ Complete Kimball dimensional model  
✅ 3.9M+ transactions across 4 years  
✅ Realistic customer behavior patterns  
✅ Semantic views for analytics  
✅ All operations Snowflake CLI verified  
✅ Incremental load strategy implemented  
✅ Ready for Cortex Analyst integration  

**Status: PRODUCTION READY** 🎿⛷️

---

## Files Created

- Database setup: `sql/setup_ski_resort_db.sql`
- Data generation: `data_generation/generate_missing_data.py`
- Connection helper: `data_generation/snowflake_connection.py`
- DBT project: `dbt_ski_resort/` (complete with 17 models)
- Verification: `scripts/verify_setup.sh`
- Documentation: `README.md`, `dbt_ski_resort/README.md`

---

Built with Snowflake CLI • DBT • Python • Snowpark
