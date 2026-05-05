with source as (
    select * from {{ source('raw', 'tmdb_metadata') }}
),

renamed as (
    select
        tmdb_id,
        movie_id,
        title,
        release_date,
        runtime,
        budget,
        revenue,
        popularity,
        vote_average,
        vote_count,
        original_language,
        overview,
        status,
        loaded_at,

        -- Extract genre names cleanly
        array_agg(distinct g.value:name::string)
            over (partition by tmdb_id)          as genre_names,

        -- Extract production company names cleanly
        array_agg(distinct p.value:name::string)
            over (partition by tmdb_id)          as production_company_names

    from source,
        lateral flatten(input => parse_json(genres)) g,
        lateral flatten(input => parse_json(production_companies)) p
)

select distinct * from renamed