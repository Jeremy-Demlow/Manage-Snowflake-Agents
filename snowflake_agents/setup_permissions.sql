-- =============================================================================
-- AI OBSERVABILITY & AGENT PERMISSIONS SETUP
-- =============================================================================
-- Run this script to set up required permissions for:
--   1. Creating and deploying agents
--   2. AI Observability / TruLens integration
--   3. Monitoring and evaluation
--
-- Run as ACCOUNTADMIN or a role with MANAGE GRANTS privilege
-- =============================================================================

-- Use your target role (adjust as needed)
USE ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- 1. CREATE DATABASES AND SCHEMAS
-- -----------------------------------------------------------------------------

-- Agent deployment database
CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.AGENTS;

-- Dev environment (separate for testing)
CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_DEV;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_DEV.AGENTS;

-- Staging environment
CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_STAGING;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_STAGING.AGENTS;

-- -----------------------------------------------------------------------------
-- 2. GRANT CORTEX_USER DATABASE ROLE
-- -----------------------------------------------------------------------------
-- Required for AI Observability and Cortex functions

-- Grant to your role (adjust role name as needed)
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE ACCOUNTADMIN;

-- If you have a service account role, grant there too:
-- GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE <YOUR_SERVICE_ROLE>;

-- -----------------------------------------------------------------------------
-- 3. AI OBSERVABILITY APPLICATION ROLES
-- -----------------------------------------------------------------------------
-- Required to view and manage AI observability data

-- For viewing observability events
GRANT APPLICATION ROLE SNOWFLAKE.AI_OBSERVABILITY_EVENTS_LOOKUP TO ROLE ACCOUNTADMIN;

-- For admin access (delete events, manage data)
GRANT APPLICATION ROLE SNOWFLAKE.AI_OBSERVABILITY_ADMIN TO ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- 4. AGENT CREATION PRIVILEGES
-- -----------------------------------------------------------------------------

-- Grant privileges to create agents in each environment
GRANT CREATE AGENT ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE ACCOUNTADMIN;
GRANT CREATE AGENT ON SCHEMA SNOWFLAKE_INTELLIGENCE_DEV.AGENTS TO ROLE ACCOUNTADMIN;
GRANT CREATE AGENT ON SCHEMA SNOWFLAKE_INTELLIGENCE_STAGING.AGENTS TO ROLE ACCOUNTADMIN;

-- Grant privileges for external agents (TruLens)
GRANT CREATE EXTERNAL AGENT ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE ACCOUNTADMIN;
GRANT CREATE EXTERNAL AGENT ON SCHEMA SNOWFLAKE_INTELLIGENCE_DEV.AGENTS TO ROLE ACCOUNTADMIN;

-- Task privileges (for evaluation runs)
GRANT CREATE TASK ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE ACCOUNTADMIN;
GRANT CREATE TASK ON SCHEMA SNOWFLAKE_INTELLIGENCE_DEV.AGENTS TO ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- 5. DATA ACCESS (Semantic Views)
-- -----------------------------------------------------------------------------

-- Grant access to the ski resort semantic views
GRANT USAGE ON DATABASE SKI_RESORT_DB TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA SKI_RESORT_DB.SEMANTIC TO ROLE ACCOUNTADMIN;
GRANT SELECT ON ALL VIEWS IN SCHEMA SKI_RESORT_DB.SEMANTIC TO ROLE ACCOUNTADMIN;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA SKI_RESORT_DB.SEMANTIC TO ROLE ACCOUNTADMIN;

-- Grant access to marts for agent tools
GRANT USAGE ON SCHEMA SKI_RESORT_DB.MARTS TO ROLE ACCOUNTADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA SKI_RESORT_DB.MARTS TO ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- 6. WAREHOUSE ACCESS
-- -----------------------------------------------------------------------------

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- 7. VERIFY SETUP
-- -----------------------------------------------------------------------------

-- Check database roles
SHOW GRANTS TO ROLE ACCOUNTADMIN;

-- List agents (should be empty initially)
SHOW AGENTS IN SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS;

-- Check AI observability access
SELECT COUNT(*) FROM SNOWFLAKE.LOCAL.AI_OBSERVABILITY_EVENTS;

-- Done!
SELECT 'Permissions setup complete!' AS STATUS;
