with director as (
    select
        tmdb_id,
        media_type,
        person_name as director_name,
        load_ts,
        row_number() over (
            partition by tmdb_id, media_type
            order by person_name
        ) as rn
    from {{ ref('stg_tmdb_credits_crew') }}
    where job = 'Director'
),

top_cast as (
    select
        tmdb_id,
        media_type,
        listagg(person_name, ', ') within group (order by cast_order) as top_cast
    from {{ ref('stg_tmdb_credits_cast') }}
    where cast_order <= 2
    group by 1, 2
),

base as (
    select distinct
        tmdb_id,
        media_type
    from {{ ref('stg_tmdb_credits_crew') }}

    union

    select distinct
        tmdb_id,
        media_type
    from {{ ref('stg_tmdb_credits_cast') }}
),

final as (
    select
        b.tmdb_id,
        b.media_type,
        d.director_name,
        t.top_cast,
        d.load_ts
    from base b
    left join director d
        on b.tmdb_id = d.tmdb_id
        and b.media_type = d.media_type
        and d.rn = 1
    left join top_cast t
        on b.tmdb_id = t.tmdb_id
        and b.media_type = t.media_type
)

select *
from final