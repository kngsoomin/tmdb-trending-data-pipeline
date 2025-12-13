-- This script creates a TEMP table in the STAGE schema
-- It flattens the RAW payload and extracts structured columns

CREATE OR REPLACE TRANSIENT TABLE DEV.STG.TMDB_TRENDING_STG AS
WITH flattened AS (
    SELECT
        r.LOAD_DATE,
        r.LOAD_TS,
        r.SOURCE,
        r.MEDIA_TYPE,
        r.TIME_WINDOW,
        f.value     AS ITEM
    FROM DEV.RAW.TMDB_TRENDING_RAW r,
        LATERAL FLATTEN(INPUT => r.PAYLOAD) f 
    WHERE r.LOAD_DATE = TO_DATE('{{ ds }}')
) 
SELECT
    -- Keys
    LOAD_DATE                       AS TRENDING_DATE,
    TIME_WINDOW,
    MEDIA_TYPE,

    -- Core identifier
    ITEM:id::NUMBER                 AS TMDB_ID,
    COALESCE(
        ITEM:title::STRING,
        ITEM:name::STRING
    )                               AS TITLE,
    
    -- Additional metadata
    ITEM:original_title::STRING     AS ORIGINAL_TITLE,
    ITEM:original_language::STRING  AS ORIGINAL_LANGUAGE,
    TRY_TO_DATE(ITEM:release_date::STRING)  AS RELEASE_DATE,
    
    -- Metrics
    ITEM:popularity::FLOAT          AS POPULARITY,
    ITEM:vote_count::NUMBER         AS VOTE_COUNT,
    ITEM:vote_average::FLOAT        AS VOTE_AVERAGE,
    LOAD_TS,

    -- Fields for future analysis
    ITEM:adult::BOOLEAN                AS IS_ADULT,
    ITEM:original_name::STRING         AS ORIGINAL_NAME,
    ITEM:poster_path::STRING           AS POSTER_PATH,
    ITEM:backdrop_path::STRING         AS BACKDROP_PATH
    
FROM flattened;