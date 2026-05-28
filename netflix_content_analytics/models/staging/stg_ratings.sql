{{
    config(
        materialized='incremental',
        unique_key=['user_id', 'movie_id'],
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}
 
with source as (
    select * from {{ source('raw', 'ratings') }}
 
    {% if is_incremental() %}
        -- Only process ratings newer than the latest one already loaded
        -- This is the critical filter — 32M rows means this saves real time
        where timestamp > (
            select max(rated_at)
            from {{ this }}
        )
    {% endif %}
),
 
renamed as (
    select
        user_id,
        movie_id,
        rating,
        timestamp                                              as rated_at,
        to_timestamp(timestamp)                                as rated_at_ts
    from source
)
 
select * from renamed