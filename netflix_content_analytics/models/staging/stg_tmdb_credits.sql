with source as (
    select
        movie_id,
        person_id,
        name,
        role,
        character_name,
        cast_order,
        popularity,
        loaded_at
    from {{ source('raw', 'tmdb_credits') }}
),

deduped as (
    select *
    from source
    qualify row_number() over (
        partition by movie_id, person_id, role
        order by loaded_at desc
    ) = 1
)

select * from deduped