{% macro snowflake__get_replace_iceberg_table_sql(relation, sql) %}

    create or replace iceberg table {{ relation }}
        catalog = {{ config.get("catalog") }}
        external_volume = {{ config.get("external_volume") }}
        base_location = {{ config.get("base_location") }}
        as (
            {{ sql }}
        )
    ;
{% endmacro %}
