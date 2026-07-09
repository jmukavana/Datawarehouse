{# yyyymmdd integer date key; extract() keeps it portable across DuckDB and BigQuery. #}
{% macro to_date_key(column) -%}
cast(
    extract(year from {{ column }}) * 10000
    + extract(month from {{ column }}) * 100
    + extract(day from {{ column }})
    as integer
)
{%- endmacro %}
