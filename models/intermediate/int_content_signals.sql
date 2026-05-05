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

rating_signals as (
    select
        movie_id,
        count(*)                                as rating_count,
        avg(rating)                             as avg_rating,
        stddev(rating)                          as rating_stddev
    from ratings
    group by movie_id
),

tag_signals as (
    select
        movie_id,
        count(*)                                as tag_count
    from tags
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

        --Quality signal: vote average weighted by vote count
        case
            when t.vote_count > 0
            then t.vote_average * ln(t.vote_count + 1)
            else 0
        end                                     as quality_signal,

        -- Engagement signal: rating volume weighted by avg rating
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
    left join tmdb t on m.movie_id = t.movie_id
    left join rating_signals r on m.movie_id = r.movie_id
    left join tag_signals tg on m.movie_id = tg.movie_id
)

select * from joined