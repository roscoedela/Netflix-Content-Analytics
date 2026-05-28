
{{
    config(
        materialized='table'
    )
}}
 
-- Intentionally kept as table materialization
-- LINKS is a static ID mapping between MovieLens and TMDB
-- It does not change week to week — incremental adds no value here
-- ADR: 005_dbt_incremental_strategy
 
with source as (
    select * from {{ source('raw', 'links') }}
),
 
renamed as (
    select
        movie_id,
        imdb_id,
        tmdb_id
    from source
)
 
select * from renamed