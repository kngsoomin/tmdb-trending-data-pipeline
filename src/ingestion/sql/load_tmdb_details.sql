DELETE FROM DEV.RAW.TMDB_DETAILS_RAW
WHERE TMDB_ID = '{{ tmdb_id }}'
  AND MEDIA_TYPE = '{{ media_type }}';

COPY INTO DEV.RAW.TMDB_DETAILS_RAW (
    TMDB_ID,
    MEDIA_TYPE,
    PAYLOAD,
    LOAD_TS
)
FROM (
    SELECT
        $1:tmdb_id::number,
        $1:media_type::string,
        $1:data,
        TO_TIMESTAMP_NTZ($1:load_ts::string)
    FROM @DEV.RAW.TMDB_DETAILS_STAGE
    (FILE_FORMAT => DEV.RAW.TMDB_JSON_FF)
)
PATTERN = '.*{{ file_name }}.*'
ON_ERROR = 'ABORT_STATEMENT';