{% macro check_duplicates(relation, unique_column) %}

with duplicates as (
    SELECT
        {{ unique_column | join(', ') }},
        count(*) as row_count
    FROM {{ relation }}
    GROUP BY {{unique_column | join(', ') }}
    having count(*) > 1
)

SELECT * FROM duplicates

{% endmacro %}