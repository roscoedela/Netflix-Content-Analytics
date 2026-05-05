with signals as (
    select * from {{ ref('int_content_signals') }}
),

normalized as (
    select
        *,

        -- Normalize each signal to 0-1 scale
        (quality_signal - min(quality_signal) over ())
            / nullif(max(quality_signal) over () - min(quality_signal) over (), 0)
                                                as quality_score,

        (engagement_signal - min(engagement_signal) over ())
            / nullif(max(engagement_signal) over () - min(engagement_signal) over (), 0)
                                                as engagement_score,

        case
            when efficiency_ratio is not null
            then (efficiency_ratio - min(efficiency_ratio) over ())
                / nullif(max(efficiency_ratio) over () - min(efficiency_ratio) over (), 0)
            else null
        end                                     as efficiency_score

    from signals
),

final as (
    select
        movie_id,
        movie_title,
        title_clean,
        release_year,
        genre_names,
        production_company_names,
        original_language,
        vote_average,
        vote_count,
        popularity,
        budget,
        revenue,
        runtime,
        rating_count,
        avg_rating,
        rating_stddev,
        tag_count,
        quality_score,
        engagement_score,
        efficiency_score,

        -- Content Value Index: weighted combination of three signals
        round(
            (coalesce(quality_score, 0) * 0.4)
            + (coalesce(engagement_score, 0) * 0.4)
            + (coalesce(efficiency_score, 0) * 0.2)
        , 4)                                    as content_value_index

    from normalized
)

select * from final
order by content_value_index desc