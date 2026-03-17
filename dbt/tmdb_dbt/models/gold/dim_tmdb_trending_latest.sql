with ranked as (

    select
        dt,
        run_id,
        object_key,
        raw_loaded_at,
        tmdb_id,
        media_type,
        title,
        name,
        display_title,
        original_title,
        original_name,
        original_display_title,
        overview,
        original_language,
        popularity,
        vote_average,
        vote_count,
        adult,
        video,
        poster_path,
        backdrop_path,
        release_date,
        first_air_date,
        genre_ids,
        raw_item_payload,

        row_number() over (
            partition by tmdb_id, media_type
            order by dt desc, raw_loaded_at desc, run_id desc
        ) as row_num

    from {{ ref('stg_tmdb_trending_daily') }}

)

select
    dt,
    run_id,
    object_key,
    raw_loaded_at,
    tmdb_id,
    media_type,
    title,
    name,
    display_title,
    original_title,
    original_name,
    original_display_title,
    overview,
    original_language,
    popularity,
    vote_average,
    vote_count,
    adult,
    video,
    poster_path,
    backdrop_path,
    release_date,
    first_air_date,
    genre_ids,
    raw_item_payload

from ranked
where row_num = 1