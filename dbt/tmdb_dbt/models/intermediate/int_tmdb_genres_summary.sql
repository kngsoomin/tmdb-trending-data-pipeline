select
    tmdb_id,
    media_type,
    listagg(genre_name, ', ') within group (order by genre_name) as genres
from {{ ref('stg_tmdb_details_genres') }}
group by 1,2