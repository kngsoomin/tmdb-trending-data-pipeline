PUT file:///tmp/tmdb_trending_{{ ds }}.json 
    @DEV.RAW.TMDB_TRENDING_STAGE
    AUTO_COMPRESS = TRUE
    OVERWRITE = TRUE;