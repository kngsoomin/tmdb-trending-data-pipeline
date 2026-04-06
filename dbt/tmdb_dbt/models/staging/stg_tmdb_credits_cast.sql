with source as (
    select *
    from {{ source('raw', 'tmdb_credits_raw') }}
),

flattened as (
    select
        r.tmdb_id,
        r.media_type,
        r.load_ts,
        f.value as cast_member
    from source r,
        lateral flatten(input => r.payload:cast) f
),

final as (
    select
        tmdb_id,
        media_type,
        cast_member:id::number as person_id,
        cast_member:name::string as person_name,
        cast_member:character::string as character_name,
        cast_member:order::number as cast_order,
        load_ts
    from flattened
)

select *
from final