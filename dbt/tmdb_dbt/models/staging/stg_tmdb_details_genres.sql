with source as (
    select *
    from {{ source('raw', 'tmdb_details_raw') }}
),

flattened as (
    select
        r.tmdb_id,
        r.media_type,
        r.load_ts,
        f.value as genre
    from source r,
        lateral flatten(input => r.payload:genres) f
),

final as (
    select
        tmdb_id,
        media_type,
        genre:id::number as genre_id,
        genre:name::string as genre_name,
        load_ts
    from flattened
)

select *
from final