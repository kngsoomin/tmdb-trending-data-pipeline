DELETE FROM DEV.RAW.TMDB_TRENDING_RAW
WHERE LOAD_DATE = TO_DATE('{{ ds }}')
    AND MEDIA_TYPE = 'all'
    AND TIME_WINDOW = 'day';

COPY INTO DEV.RAW.TMDB_TRENDING_RAW (
    MEDIA_TYPE,
    TIME_WINDOW,
    PAYLOAD,
    LOAD_DATE
)
FROM (
    SELECT
        $1:media_type::string   AS MEDIA_TYPE,
        $1:time_window::string  AS TIME_WINDOW,
        $1:results              AS PAYLOAD,
        TO_DATE('{{ ds }}')     AS LOAD_DATE
    FROM @DEV.RAW.TMDB_TRENDING_STAGE (FILE_FORMAT => DEV.RAW.TMDB_JSON_FF)
)
PATTERN = '.*tmdb_trending_{{ ds }}.*'
ON_ERROR = 'ABORT_STATEMENT';