-- Ski Resort Analytics Database Setup
-- Creates database and schemas for the ski resort analytics platform

-- Create main database
CREATE DATABASE IF NOT EXISTS SKI_RESORT_DB
COMMENT = 'Ski resort analytics platform with 4 years of operational data';

-- Create schemas
CREATE SCHEMA IF NOT EXISTS SKI_RESORT_DB.RAW
COMMENT = 'Landing zone for raw transactional data from ski resort operations';

CREATE SCHEMA IF NOT EXISTS SKI_RESORT_DB.STAGING
COMMENT = 'Cleaned and type-safe staging tables from raw data';

CREATE SCHEMA IF NOT EXISTS SKI_RESORT_DB.MARTS
COMMENT = 'Kimball dimensional model - fact and dimension tables';

CREATE SCHEMA IF NOT EXISTS SKI_RESORT_DB.SEMANTIC
COMMENT = 'Semantic views for Snowflake Cortex Analyst agents';

-- Grant usage to appropriate roles
GRANT USAGE ON DATABASE SKI_RESORT_DB TO ROLE SYSADMIN;
GRANT USAGE ON ALL SCHEMAS IN DATABASE SKI_RESORT_DB TO ROLE SYSADMIN;
GRANT ALL ON SCHEMA SKI_RESORT_DB.RAW TO ROLE SYSADMIN;
GRANT ALL ON SCHEMA SKI_RESORT_DB.STAGING TO ROLE SYSADMIN;
GRANT ALL ON SCHEMA SKI_RESORT_DB.MARTS TO ROLE SYSADMIN;
GRANT ALL ON SCHEMA SKI_RESORT_DB.SEMANTIC TO ROLE SYSADMIN;

-- Show created schemas for verification
SHOW SCHEMAS IN DATABASE SKI_RESORT_DB;
