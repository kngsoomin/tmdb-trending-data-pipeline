with flattened as (
    select
        r.snapshot_date,
        r.load_ts,
        r.media_type,
        r.time_window,
        f.value as item
    from {{ source('raw', 'tmdb_trending_raw') }} r,
        lateral flatten(input => r.payload) f
)

select
    snapshot_date as trending_date,
    time_window,
    media_type,

    item:id::number as tmdb_id,
    coalesce(item:title::string, item:name::string) as title,

    item:original_title::string as original_title,
    item:original_language::string as original_language,
    try_to_date(item:release_date::string) as release_date,

    item:popularity::float as popularity,
    item:vote_count::number as vote_count,
    item:vote_average::float as vote_average,
    load_ts,

    item:adult::boolean as is_adult,
    item:original_name::string as original_name,
    item:poster_path::string as poster_path,
    item:backdrop_path::string as backdrop_path
from flattened