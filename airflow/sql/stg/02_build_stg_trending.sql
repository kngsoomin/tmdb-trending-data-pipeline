-- Build STG data for the execution date (idempotent per date)

BEGIN;

-- Remove existing rows for the date
DELETE FROM DEV.STG.TMDB_TRENDING_STG
WHERE TRENDING_DATE = TO_DATE('{{ ds }}');

INSERT INTO DEV.STG.TMDB_TRENDING_STG (
    TRENDING_DATE,
    TIME_WINDOW,
    MEDIA_TYPE,
    TMDB_ID,
    TITLE,
    ORIGINAL_TITLE,
    ORIGINAL_LANGUAGE,
    RELEASE_DATE,
    POPULARITY,
    VOTE_COUNT,
    VOTE_AVERAGE,
    LOAD_TS,
    IS_ADULT,
    ORIGINAL_NAME,
    POSTER_PATH,
    BACKDROP_PATH
)
WITH flattened AS (
    SELECT
        r.LOAD_DATE,
        r.LOAD_TS,
        r.MEDIA_TYPE,
        r.TIME_WINDOW,
        f.value AS ITEM
    FROM DEV.RAW.TMDB_TRENDING_RAW r,
         LATERAL FLATTEN(INPUT => r.PAYLOAD) f
    WHERE r.LOAD_DATE = TO_DATE('{{ ds }}')
)
SELECT
    LOAD_DATE                                   AS TRENDING_DATE,
    TIME_WINDOW,
    MEDIA_TYPE,

    ITEM:id::NUMBER                             AS TMDB_ID,
    COALESCE(ITEM:title::STRING, ITEM:name::STRING) AS TITLE,

    ITEM:original_title::STRING                 AS ORIGINAL_TITLE,
    ITEM:original_language::STRING              AS ORIGINAL_LANGUAGE,
    TRY_TO_DATE(ITEM:release_date::STRING)      AS RELEASE_DATE,

    ITEM:popularity::FLOAT                      AS POPULARITY,
    ITEM:vote_count::NUMBER                     AS VOTE_COUNT,
    ITEM:vote_average::FLOAT                    AS VOTE_AVERAGE,
    LOAD_TS,

    ITEM:adult::BOOLEAN                         AS IS_ADULT,
    ITEM:original_name::STRING                  AS ORIGINAL_NAME,
    ITEM:poster_path::STRING                    AS POSTER_PATH,
    ITEM:backdrop_path::STRING                  AS BACKDROP_PATH
FROM flattened;

COMMIT;
