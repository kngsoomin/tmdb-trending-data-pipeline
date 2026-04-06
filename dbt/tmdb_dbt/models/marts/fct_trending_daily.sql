{{ config(materialized='table') }}

select
    trending_date,
    time_window,
    tmdb_id,
    media_type,
    popularity,
    vote_count,
    vote_average,
    load_ts
from {{ ref('stg_tmdb_trending') }}