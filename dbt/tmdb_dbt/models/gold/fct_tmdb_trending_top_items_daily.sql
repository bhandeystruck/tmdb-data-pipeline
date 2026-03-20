with deduplicated as (

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
        raw_loaded_at,
        run_id,

        row_number() over (
            partition by dt, media_type, tmdb_id
            order by raw_loaded_at desc, run_id desc
        ) as dedupe_row_num
    from {{ ref('stg_tmdb_trending_daily') }}

),

latest_per_item as (

    select
        dt,
        media_type,
        tmdb_id,
        display_title,
        popularity,
        vote_average,
        vote_count,
        poster_path,
        backdrop_path
    from deduplicated
    where dedupe_row_num = 1

),

ranked as (

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
    from latest_per_item

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