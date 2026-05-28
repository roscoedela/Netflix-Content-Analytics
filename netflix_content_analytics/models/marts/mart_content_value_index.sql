{{
    config(
        materialized='table'
    )
}}
 
-- Intentionally kept as table materialization
--
-- The CVI uses min-max normalization via window functions (over ())
-- which require the full dataset to calculate correct min and max values.
-- On an incremental run, only new rows would be visible, making the
-- normalization incorrect — a film's score would be relative to the
-- batch, not the full history.
--
-- The performance win comes from stg_ratings being incremental (32M rows).
-- By the time data reaches this mart it is already aggregated to one row
-- per movie, so a full table rebuild here is fast regardless.
--
-- ADR: 005_dbt_incremental_strategy
 
with signals as (
    select * from {{ ref('int_content_signals') }}
),
 
normalized as (
    select
        *,
 
        -- Normalize each signal to 0-1 scale using min-max scaling
        -- Requires full dataset — this is why the mart stays as a table
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
 
        -- Content tier: indicates which signals were available for scoring
        case
            when efficiency_score is not null then 'Full Score'
            else 'No Efficiency'
        end                                     as content_tier,
 
        -- CVI with smart weight redistribution based on data availability
        round(
            case
                when efficiency_score is not null
                then ((quality_score * 0.4) + (engagement_score * 0.4) + (efficiency_score * 0.2)) * 100
                else ((quality_score * 0.5) + (engagement_score * 0.5)) * 100
            end
        , 4)                                    as content_value_index
 
    from normalized
)
 
select * from final
order by content_value_index desc