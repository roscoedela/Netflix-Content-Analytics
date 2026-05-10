with source as (
    select * from {{ source('raw', 'movies') }}
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