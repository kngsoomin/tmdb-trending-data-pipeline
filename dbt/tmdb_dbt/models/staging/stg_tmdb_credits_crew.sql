with source as (
    select *
    from {{ source('raw', 'tmdb_credits_raw') }}
),

flattened as (
    select
        r.tmdb_id,
        r.media_type,
        r.load_ts,
        f.value as crew_member
    from source r,
        lateral flatten(input => r.payload:crew) f
),

final as (
    select
        tmdb_id,
        media_type,
        crew_member:id::number as person_id,
        crew_member:name::string as person_name,
        crew_member:department::string as department,
        crew_member:job::string as job,
        load_ts
    from flattened
)

select *
from final