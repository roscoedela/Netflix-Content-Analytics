select
    movie_id,
    content_value_index
from {{ ref('mart_content_value_index') }}
where
    content_value_index is null
    or content_value_index < 0
    or content_value_index > 100