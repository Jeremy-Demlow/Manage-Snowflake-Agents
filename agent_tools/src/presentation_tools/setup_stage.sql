-- =============================================================================
-- Setup for SEND_CONVERSATION_REPORT Tool
-- =============================================================================
-- Creates the required stage for storing generated PowerPoint files.
-- Files are uploaded here temporarily and accessed via presigned URLs.
-- =============================================================================

USE DATABASE AGENT_TOOLS_CENTRAL;
USE SCHEMA AGENT_TOOLS;

-- =============================================================================
-- 1. CREATE STAGE FOR REPORT FILES
-- Using SNOWFLAKE_SSE encryption (server-side, not client-side)
-- This is required for GET_PRESIGNED_URL to work
-- =============================================================================
CREATE STAGE IF NOT EXISTS REPORT_FILES_STAGE
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Stage for generated PowerPoint reports - files accessed via presigned URLs';

-- =============================================================================
-- 2. GRANT PERMISSIONS (adjust role as needed)
-- =============================================================================
-- Grant to agent execution role
GRANT READ, WRITE ON STAGE REPORT_FILES_STAGE TO ROLE AGENT_TOOLS_ROLE;

-- If using a service account for the tool
-- GRANT READ, WRITE ON STAGE REPORT_FILES_STAGE TO ROLE SERVICE_ACCOUNT_ROLE;

-- =============================================================================
-- 3. CLEANUP TASK (Optional - removes old files after 7 days)
-- =============================================================================
CREATE OR REPLACE TASK CLEANUP_OLD_REPORTS
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 2 * * * America/Los_Angeles'  -- 2am PST daily
AS
    REMOVE @REPORT_FILES_STAGE PATTERN='.*si_report_.*\\.pptx'
    -- Note: Snowflake doesn't support age-based removal directly
    -- Consider implementing a more sophisticated cleanup procedure
;

-- Don't enable by default - uncomment if needed
-- ALTER TASK CLEANUP_OLD_REPORTS RESUME;

-- =============================================================================
-- VERIFICATION
-- =============================================================================
SHOW STAGES LIKE 'REPORT_FILES_STAGE' IN SCHEMA AGENT_TOOLS_CENTRAL.AGENT_TOOLS;

SELECT 'SEND_CONVERSATION_REPORT stage setup complete' AS status;
