{% macro snowflake__get_create_iceberg_table_as_sql(relation, sql) -%}

    create iceberg table {{ relation }}
        catalog = '{{ config.get("catalog") }}'
        external_volume = {{ config.get("external_volume") }}
        base_location = '{{ config.get("base_location") }}'

        as (
            {{ sql }}
        )

{%- endmacro %}
