{% macro snowflake__refresh_iceberg_table(relation) -%}
    {{- log('Applying REFRESH to: ' ~ relation) -}}

    alter iceberg table {{ relation }} refresh
{%- endmacro %}
