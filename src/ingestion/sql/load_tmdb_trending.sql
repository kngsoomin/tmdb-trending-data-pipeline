DELETE FROM DEV.RAW.TMDB_TRENDING_RAW
WHERE SNAPSHOT_DATE = TO_DATE('{{ snapshot_date }}')
  AND MEDIA_TYPE = 'all'
  AND TIME_WINDOW = 'day';

COPY INTO DEV.RAW.TMDB_TRENDING_RAW (
    MEDIA_TYPE,
    TIME_WINDOW,
    PAYLOAD,
    SNAPSHOT_DATE,
    LOAD_TS
)
FROM (
    SELECT
        $1:media_type::string,
        $1:time_window::string,
        $1:results,
        TO_DATE('{{ snapshot_date }}'),
        TO_TIMESTAMP_NTZ($1:load_ts::string)
    FROM @DEV.RAW.TMDB_TRENDING_STAGE
    (FILE_FORMAT => DEV.RAW.TMDB_JSON_FF)
)
PATTERN = '.*{{ file_name }}.*'
ON_ERROR = 'ABORT_STATEMENT';