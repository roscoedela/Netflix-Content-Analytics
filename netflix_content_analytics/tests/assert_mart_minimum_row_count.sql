-- Asserts that the mart_content_value_index table has at least 85,000 rows, indicating that the data pipeline is populating the table as expected. This threshold is based on the known size of the source datasets and typical row counts observed during development. If this assertion fails, it may indicate an issue with upstream transformations or data loading processes that needs to be investigated.
select count(*) as row_count
from {{ ref('mart_content_value_index') }}
having count(*) < 85000