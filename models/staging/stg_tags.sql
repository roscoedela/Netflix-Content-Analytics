with source as (
    select * from {{ source('raw', 'tags') }}
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