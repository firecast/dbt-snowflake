{% macro snowflake__get_replace_sql(existing_relation, target_relation, sql) %}

    {% if existing_relation.is_dynamic_table and target_relation.is_dynamic_table %}
        {{ snowflake__get_replace_dynamic_table_sql(target_relation, sql) }}

    {% elif existing_relation.is_iceberg_table and target_relation.is_iceberg_table %}
        {{ snowflake__get_replace_iceberg_table_sql(target_relation, sql) }}

    {% else %}
        {{ default__get_replace_sql(existing_relation, target_relation, sql) }}

    {% endif %}

{% endmacro %}
