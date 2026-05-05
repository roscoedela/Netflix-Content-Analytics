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