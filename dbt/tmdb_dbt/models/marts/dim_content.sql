{{ config(materialized='table') }}

with trending as (
    select distinct
        tmdb_id,
        media_type,
        title,
        original_title,
        original_language,
        release_date
    from {{ ref('stg_tmdb_trending') }}
    where tmdb_id is not null
),

details as (
    select
        tmdb_id,
        media_type,
        title as details_title,
        original_title as details_original_title,
        original_language as details_original_language,
        release_date as details_release_date,
        runtime,
        imdb_id
    from {{ ref('stg_tmdb_details') }}
),

credits as (
    select
        tmdb_id,
        media_type,
        director_name,
        top_cast
    from {{ ref('int_tmdb_credits_summary') }}
),

genres as (
    select
        tmdb_id,
        media_type,
        genres
    from {{ ref('int_tmdb_genres_summary') }}
),

final as (
    select
        t.tmdb_id,
        t.media_type,

        coalesce(d.details_title, t.title) as title,
        coalesce(d.details_original_title, t.original_title) as original_title,
        coalesce(d.details_original_language, t.original_language) as original_language,
        coalesce(d.details_release_date, t.release_date) as release_date,

        d.runtime,
        d.imdb_id,

        c.director_name,
        c.top_cast,
        g.genres

    from trending t
    left join details d
        on t.tmdb_id = d.tmdb_id
        and t.media_type = d.media_type
    left join credits c
        on t.tmdb_id = c.tmdb_id
        and t.media_type = c.media_type
    left join genres g
        on t.tmdb_id = g.tmdb_id
        and t.media_type = g.media_type
)

select *
from final