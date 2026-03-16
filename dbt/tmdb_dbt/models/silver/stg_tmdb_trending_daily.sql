with raw as (

    select
        dt,
        run_id,
        object_key,
        loaded_at as raw_loaded_at,
        payload
    from {{ source('bronze', 'tmdb_trending_daily_raw') }}

),

flattened as (

    select
        raw.dt,
        raw.run_id,
        raw.object_key,
        raw.raw_loaded_at,
        result.value as item
    from raw,
    lateral flatten(input => raw.payload:results) as result

)

select
    dt,
    run_id,
    object_key,
    raw_loaded_at,

    item:id::number as tmdb_id,
    item:media_type::string as media_type,

    item:title::string as title,
    item:name::string as name,
    coalesce(item:title::string, item:name::string) as display_title,

    item:original_title::string as original_title,
    item:original_name::string as original_name,
    coalesce(item:original_title::string, item:original_name::string) as original_display_title,

    item:overview::string as overview,
    item:original_language::string as original_language,

    item:popularity::float as popularity,
    item:vote_average::float as vote_average,
    item:vote_count::number as vote_count,

    item:adult::boolean as adult,
    item:video::boolean as video,

    item:poster_path::string as poster_path,
    item:backdrop_path::string as backdrop_path,

    item:release_date::date as release_date,
    item:first_air_date::date as first_air_date,

    item:genre_ids as genre_ids,
    item as raw_item_payload

from flattened