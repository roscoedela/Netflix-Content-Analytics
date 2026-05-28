{{
    config(
        materialized='incremental',
        unique_key='movie_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}
 
with movies as (
    select * from {{ ref('stg_movies') }}
),
 
ratings as (
    select * from {{ ref('stg_ratings') }}
),
 
tags as (
    select * from {{ ref('stg_tags') }}
),
 
tmdb as (
    select * from {{ ref('stg_tmdb_metadata') }}
),
 
{% if is_incremental() %}
-- On incremental runs, only recompute signals for movies that have
-- received new ratings, new tags, or updated TMDB metadata this week
-- This avoids reprocessing all 32M ratings rows on every run
changed_movies as (
    select distinct movie_id from {{ ref('stg_ratings') }}
    where rated_at > (select max(rated_at) from {{ ref('stg_ratings') }}
                      where movie_id in (select movie_id from {{ this }}))
 
    union
 
    select distinct movie_id from {{ ref('stg_tags') }}
    where tagged_at > (select max(tagged_at) from {{ ref('stg_tags') }}
                       where movie_id in (select movie_id from {{ this }}))
 
    union
 
    select distinct movie_id from {{ ref('stg_tmdb_metadata') }}
    where updated_at > (select max(updated_at) from {{ ref('stg_tmdb_metadata') }}
                        where movie_id in (select movie_id from {{ this }}))
),
{% endif %}
 
rating_signals as (
    select
        movie_id,
        count(*)                                as rating_count,
        avg(rating)                             as avg_rating,
        stddev(rating)                          as rating_stddev
    from ratings
 
    {% if is_incremental() %}
        where movie_id in (select movie_id from changed_movies)
    {% endif %}
 
    group by movie_id
),
 
tag_signals as (
    select
        movie_id,
        count(*)                                as tag_count
    from tags
 
    {% if is_incremental() %}
        where movie_id in (select movie_id from changed_movies)
    {% endif %}
 
    group by movie_id
),
 
joined as (
    select
        m.movie_id,
        m.movie_title,
        m.title_clean,
        m.release_year,
        m.genres_array,
        t.vote_average,
        t.vote_count,
        t.popularity,
        t.budget,
        t.revenue,
        t.runtime,
        t.genre_names,
        t.production_company_names,
        t.original_language,
        r.rating_count,
        r.avg_rating,
        r.rating_stddev,
        coalesce(tg.tag_count, 0)               as tag_count,
 
        -- Quality signal: vote average weighted by log of vote count
        case
            when t.vote_count > 0
            then t.vote_average * ln(t.vote_count + 1)
            else 0
        end                                     as quality_signal,
 
        -- Engagement signal: avg rating weighted by log of rating volume
        case
            when r.rating_count > 0
            then r.avg_rating * ln(r.rating_count + 1)
            else 0
        end                                     as engagement_signal,
 
        -- Efficiency signal: revenue to budget ratio
        case
            when t.budget > 0
            then t.revenue / t.budget
            else null
        end                                     as efficiency_ratio
 
    from movies m
 
    {% if is_incremental() %}
        -- On incremental runs, only join movies that changed
        inner join changed_movies cm on m.movie_id = cm.movie_id
    {% endif %}
 
    left join tmdb t         on m.movie_id = t.movie_id
    left join rating_signals r on m.movie_id = r.movie_id
    left join tag_signals tg   on m.movie_id = tg.movie_id
)
 
select * from joined