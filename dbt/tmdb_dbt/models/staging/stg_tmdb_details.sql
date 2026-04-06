with source as (
    select *
    from {{ source('raw', 'tmdb_details_raw') }}
),

final as (
    select
        tmdb_id,
        media_type,

        coalesce(payload:title::string, payload:name::string) as title,
        payload:original_title::string as original_title,
        payload:original_language::string as original_language,

        coalesce(
            try_to_date(payload:release_date::string),
            try_to_date(payload:first_air_date::string)
        ) as release_date,

        payload:runtime::number as runtime,
        payload:vote_average::float as vote_average,
        payload:vote_count::number as vote_count,
        payload:imdb_id::string as imdb_id,

        load_ts
    from source
)

select *
from final