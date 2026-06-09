with signals as (
    select * from {{ ref('int_actor_signals') }}
),

revenue_stats as (
    select
        max(nullif(avg_revenue, 0)) as max_revenue,
        min(nullif(avg_revenue, 0)) as min_revenue,
        max(avg_cvi)                as max_cvi,
        min(avg_cvi)                as min_cvi
    from signals
),

scored as (
    select
        s.person_id,
        s.name,
        s.role,
        s.film_count,
        s.avg_cvi,
        s.avg_quality,
        s.avg_engagement,
        s.avg_revenue,
        s.max_cvi,
        s.full_score_films,

        -- Normalize CVI 0-1
        case
            when r.max_cvi = r.min_cvi then 0
            else (s.avg_cvi - r.min_cvi) / nullif(r.max_cvi - r.min_cvi, 0)
        end as normalized_cvi,

        -- Normalize revenue 0-1
        case
            when r.max_revenue = r.min_revenue then 0
            when s.avg_revenue is null then 0
            else (s.avg_revenue - r.min_revenue) / nullif(r.max_revenue - r.min_revenue, 0)
        end as normalized_revenue,

        -- Cap film count at 20, normalize 0-1
        least(s.film_count, 20) / 20.0 as normalized_film_count

    from signals s
    cross join revenue_stats r
),

final as (
    select
        person_id,
        name,
        role,
        film_count,
        avg_cvi,
        avg_quality,
        avg_engagement,
        avg_revenue,
        max_cvi,
        full_score_films,
        normalized_cvi,
        normalized_revenue,
        normalized_film_count,

        round(
            (normalized_cvi * 0.4)
            + (normalized_revenue * 0.4)
            + (normalized_film_count * 0.2),
        4) as talent_value_score

    from scored
)

select * from final
order by talent_value_score desc