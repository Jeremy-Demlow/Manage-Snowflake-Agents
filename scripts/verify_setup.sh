#!/bin/bash
# Verify complete ski resort analytics setup

echo "=================================================================="
echo "SKI RESORT ANALYTICS - VERIFICATION SCRIPT"
echo "=================================================================="
echo ""

# Check database and schemas
echo "1. Checking database and schemas..."
snow sql -q "SHOW SCHEMAS IN DATABASE SKI_RESORT_DB" --connection agents_example

echo ""
echo "2. Checking RAW data tables..."
snow sql -q "
SELECT
    'CUSTOMERS' as table_name, COUNT(*) as row_count FROM SKI_RESORT_DB.RAW.CUSTOMERS
UNION ALL SELECT 'LIFT_SCANS', COUNT(*) FROM SKI_RESORT_DB.RAW.LIFT_SCANS
UNION ALL SELECT 'PASS_USAGE', COUNT(*) FROM SKI_RESORT_DB.RAW.PASS_USAGE
UNION ALL SELECT 'TICKET_SALES', COUNT(*) FROM SKI_RESORT_DB.RAW.TICKET_SALES
UNION ALL SELECT 'RENTALS', COUNT(*) FROM SKI_RESORT_DB.RAW.RENTALS
UNION ALL SELECT 'FOOD_BEVERAGE', COUNT(*) FROM SKI_RESORT_DB.RAW.FOOD_BEVERAGE
UNION ALL SELECT 'LIFTS', COUNT(*) FROM SKI_RESORT_DB.RAW.LIFTS
UNION ALL SELECT 'LOCATIONS', COUNT(*) FROM SKI_RESORT_DB.RAW.LOCATIONS
UNION ALL SELECT 'PRODUCTS', COUNT(*) FROM SKI_RESORT_DB.RAW.PRODUCTS
UNION ALL SELECT 'TICKET_TYPES', COUNT(*) FROM SKI_RESORT_DB.RAW.TICKET_TYPES
ORDER BY table_name
" --connection agents_example

echo ""
echo "3. Checking MARTS dimensional model..."
snow sql -q "
SELECT
    'dim_date' as table_name, COUNT(*) as row_count FROM SKI_RESORT_DB.MARTS_MARTS.DIM_DATE
UNION ALL SELECT 'dim_customer', COUNT(*) FROM SKI_RESORT_DB.MARTS_MARTS.DIM_CUSTOMER
UNION ALL SELECT 'dim_lift', COUNT(*) FROM SKI_RESORT_DB.MARTS_MARTS.DIM_LIFT
UNION ALL SELECT 'dim_location', COUNT(*) FROM SKI_RESORT_DB.MARTS_MARTS.DIM_LOCATION
UNION ALL SELECT 'dim_product', COUNT(*) FROM SKI_RESORT_DB.MARTS_MARTS.DIM_PRODUCT
UNION ALL SELECT 'dim_ticket_type', COUNT(*) FROM SKI_RESORT_DB.MARTS_MARTS.DIM_TICKET_TYPE
UNION ALL SELECT 'fact_lift_scans', COUNT(*) FROM SKI_RESORT_DB.MARTS_MARTS.FACT_LIFT_SCANS
UNION ALL SELECT 'fact_pass_usage', COUNT(*) FROM SKI_RESORT_DB.MARTS_MARTS.FACT_PASS_USAGE
ORDER BY table_name
" --connection agents_example

echo ""
echo "4. Checking SEMANTIC views..."
snow sql -q "SHOW VIEWS IN SCHEMA SKI_RESORT_DB.SEMANTIC" --connection agents_example

echo ""
echo "=================================================================="
echo "✓ VERIFICATION COMPLETE"
echo "=================================================================="
echo ""
echo "Summary:"
echo "  - RAW: 10 tables with transactional data"
echo "  - MARTS: 6 dimensions + 2 facts (Kimball model)"
echo "  - SEMANTIC: 2 views for analytics"
echo ""
echo "Next steps:"
echo "  1. Query semantic views: SELECT * FROM SKI_RESORT_DB.SEMANTIC.SEM_CUSTOMER_BEHAVIOR LIMIT 10"
echo "  2. Create Cortex Analyst agents"
echo "  3. Test natural language queries"
echo "=================================================================="
