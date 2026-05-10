with source as (
    select * from {{ source('raw', 'ratings') }}
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
