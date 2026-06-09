with credits as (
    select * from {{ ref('stg_tmdb_credits') }}
),

cvi as (
    select * from {{ ref('mart_content_value_index') }}
),

joined as (
    select
        c.person_id,
        c.name,
        c.role,
        c.movie_id,
        m.content_value_index,
        m.quality_score,
        m.engagement_score,
        m.revenue,
        m.content_tier
    from credits c
    inner join cvi m on c.movie_id = m.movie_id
),

aggregated as (
    select
        person_id,
        name,
        role,
        count(distinct movie_id)                    as film_count,
        round(avg(content_value_index), 4)          as avg_cvi,
        round(avg(quality_score), 4)                as avg_quality,
        round(avg(engagement_score), 4)             as avg_engagement,
        round(avg(nullif(revenue, 0)), 2)           as avg_revenue,
        max(content_value_index)                    as max_cvi,
        count(case when content_tier = 'Full Score' 
                   then 1 end)                      as full_score_films
    from joined
    group by 1, 2, 3
    having count(distinct movie_id) >= 2  -- at least 2 films to be meaningful
)

select * from aggregated