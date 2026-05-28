{{
    config(
        materialized='incremental',
        unique_key=['user_id', 'movie_id', 'tag'],
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}
 
with source as (
    select * from {{ source('raw', 'tags') }}
 
    {% if is_incremental() %}
        -- Only process tags newer than the latest one already loaded
        where timestamp > (
            select max(tagged_at)
            from {{ this }}
        )
    {% endif %}
),
 
renamed as (
    select
        user_id,
        movie_id,
        tag,
        timestamp                                              as tagged_at,
        to_timestamp(timestamp)                                as tagged_at_ts
    from source
)
 
select * from renamed