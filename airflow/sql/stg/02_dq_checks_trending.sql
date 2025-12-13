-- This script performs data quality checks on STG layer
SELECT CURRENT_ACCOUNT(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE();

-- DQ01: Check for mandatory fields 
--  TMDB_ID, TITLE
WITH invalid_mandatory AS (
    SELECT
        COUNT(*) AS invalid_count
    FROM DEV.STG.TMDB_TRENDING_STG
    WHERE TMDB_ID IS NULL
        OR TITLE IS NULL
)
SELECT 
    CASE 
        WHEN invalid_count > 0 THEN
            TO_NUMBER(1/0) -- force the task to fail
        ELSE 0
    END AS dq_mandatory_result
FROM invalid_mandatory;

-- DQ02: Check for duplicate business keys
WITH duplicates AS (
    SELECT
        TRENDING_DATE,
        TIME_WINDOW,
        MEDIA_TYPE,
        TMDB_ID,
        COUNT(*) AS cnt
    FROM DEV.STG.TMDB_TRENDING_STG
    GROUP BY TRENDING_DATE, TIME_WINDOW, MEDIA_TYPE, TMDB_ID
    HAVING COUNT(*) > 1
),
dup_summary AS (
    SELECT COUNT(*) AS duplicate_groups
    FROM duplicates
)
SELECT
    CASE
        WHEN duplicate_groups > 0 THEN
            TO_NUMBER(1 / 0)
        ELSE 0
    END AS dq_uniqueness_result
FROM dup_summary;