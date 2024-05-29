{% macro snowflake__describe_iceberg_table(relation) %}
    {%- set _iceberg_table_sql -%}
        show iceberg tables
            like '{{ relation.identifier }}'
            in schema {{ relation.database }}.{{ relation.schema }}
        ;
        select
            "name",
            "schema_name",
            "database_name",
            "text",
            "external_volume_name",
            "catalog_name",
            "base_location",
        from table(result_scan(last_query_id()))
    {%- endset %}
    {% set _iceberg_table = run_query(_iceberg_table_sql) %}

    {% do return({'iceberg_table': _iceberg_table}) %}
{% endmacro %}
