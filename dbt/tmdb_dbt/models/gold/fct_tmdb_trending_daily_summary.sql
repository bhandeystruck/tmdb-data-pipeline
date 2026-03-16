with base as (

    select
        dt,
        media_type,
        tmdb_id,
        popularity,
        vote_average,
        vote_count
    from {{ ref('stg_tmdb_trending_daily') }}

),

aggregated as (

    select
        dt,
        media_type,
        count(*) as item_count,
        count(distinct tmdb_id) as distinct_item_count,
        avg(popularity) as avg_popularity,
        avg(vote_average) as avg_vote_average,
        sum(vote_count) as total_vote_count,
        max(popularity) as max_popularity
    from base
    group by 1, 2

)

select *
from aggregated