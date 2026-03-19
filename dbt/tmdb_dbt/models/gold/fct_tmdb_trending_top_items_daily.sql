with ranked as (

    select
        dt,
        media_type,
        tmdb_id,
        display_title,
        popularity,
        vote_average,
        vote_count,
        poster_path,
        backdrop_path,
        row_number() over (
            partition by dt, media_type
            order by popularity desc, vote_count desc, tmdb_id
        ) as popularity_rank
    from {{ ref('stg_tmdb_trending_daily') }}

)

select
    dt,
    media_type,
    tmdb_id,
    display_title,
    popularity,
    vote_average,
    vote_count,
    poster_path,
    backdrop_path,
    popularity_rank
from ranked
where popularity_rank <= 10