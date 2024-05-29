{% macro snowflake__get_drop_iceberg_table_sql(relation) %}
    drop iceberg table if exists {{ relation }}
{% endmacro %}
