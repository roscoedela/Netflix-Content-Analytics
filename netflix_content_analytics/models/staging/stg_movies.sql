{{
    config(
        materialized='incremental',
        unique_key='movie_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}
 
with source as (
    select * from {{ source('raw', 'movies') }}
 
    {% if is_incremental() %}
        -- Only process movie_ids not yet in this model
        -- Handles new weekly releases without reprocessing all 60k rows
        where movie_id not in (select movie_id from {{ this }})
    {% endif %}
),
 
renamed as (
    select
        movie_id,
        title                                                           as movie_title,
        trim(regexp_replace(title, '\\(\\d{4}\\)', ''))                 as title_clean,
        regexp_substr(title, '\\((\\d{4})\\)', 1, 1, 'e', 1)::int      as release_year,
        genres                                                          as genres_raw,
        split(genres, '|')                                              as genres_array
    from source
)
 
select * from renamed