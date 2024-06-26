{% macro snowflake__get_catalog(information_schema, schemas) -%}

    {% set query %}
        with tables as (
            {{ snowflake__get_catalog_tables_sql(information_schema) }}
            {{ snowflake__get_catalog_schemas_where_clause_sql(schemas) }}
        ),
        columns as (
            {{ snowflake__get_catalog_columns_sql(information_schema) }}
            {{ snowflake__get_catalog_schemas_where_clause_sql(schemas) }}
        )
        {{ snowflake__get_catalog_results_sql() }}
    {%- endset -%}

    {{ return(run_query(query)) }}

{%- endmacro %}


{% macro snowflake__get_catalog_relations(information_schema, relations) -%}

    {% set query %}
        with tables as (
            {{ snowflake__get_catalog_tables_sql(information_schema) }}
            {{ snowflake__get_catalog_relations_where_clause_sql(relations) }}
        ),
        columns as (
            {{ snowflake__get_catalog_columns_sql(information_schema) }}
            {{ snowflake__get_catalog_relations_where_clause_sql(relations) }}
        )
        {{ snowflake__get_catalog_results_sql() }}
    {%- endset -%}

    {{ return(run_query(query)) }}

{%- endmacro %}


{% macro snowflake__get_catalog_tables_sql(information_schema) -%}
    select
        table_catalog as "table_database",
        table_schema as "table_schema",
        table_name as "table_name",
        case
            when is_dynamic = 'YES' and table_type = 'BASE TABLE' THEN 'DYNAMIC TABLE'
            when is_iceberg = 'YES' and table_type = 'BASE TABLE' THEN 'ICEBERG TABLE'
            else table_type
        end as "table_type",
        comment as "table_comment",

        -- note: this is the _role_ that owns the table
        table_owner as "table_owner",

        'Clustering Key' as "stats:clustering_key:label",
        clustering_key as "stats:clustering_key:value",
        'The key used to cluster this table' as "stats:clustering_key:description",
        (clustering_key is not null) as "stats:clustering_key:include",

        'Row Count' as "stats:row_count:label",
        row_count as "stats:row_count:value",
        'An approximate count of rows in this table' as "stats:row_count:description",
        (row_count is not null) as "stats:row_count:include",

        'Approximate Size' as "stats:bytes:label",
        bytes as "stats:bytes:value",
        'Approximate size of the table as reported by Snowflake' as "stats:bytes:description",
        (bytes is not null) as "stats:bytes:include",

        'Last Modified' as "stats:last_modified:label",
        to_varchar(convert_timezone('UTC', last_altered), 'yyyy-mm-dd HH24:MI'||'UTC') as "stats:last_modified:value",
        'The timestamp for last update/change' as "stats:last_modified:description",
        (last_altered is not null and table_type='BASE TABLE') as "stats:last_modified:include"
    from {{ information_schema }}.tables
{%- endmacro %}


{% macro snowflake__get_catalog_columns_sql(information_schema) -%}
    select
        table_catalog as "table_database",
        table_schema as "table_schema",
        table_name as "table_name",

        column_name as "column_name",
        ordinal_position as "column_index",
        data_type as "column_type",
        comment as "column_comment"
    from {{ information_schema }}.columns
{%- endmacro %}


{% macro snowflake__get_catalog_results_sql() -%}
    select *
    from tables
    join columns using ("table_database", "table_schema", "table_name")
    order by "column_index"
{%- endmacro %}


{% macro snowflake__catalog_equals(field, value) %}
    "{{ field }}" ilike '{{ value }}' and upper("{{ field }}") = upper('{{ value }}')
{% endmacro %}


{% macro snowflake__get_catalog_schemas_where_clause_sql(schemas) -%}
    where ({%- for schema in schemas -%}
        ({{ snowflake__catalog_equals('table_schema', schema) }}){%- if not loop.last %} or {% endif -%}
    {%- endfor -%})
{%- endmacro %}


{% macro snowflake__get_catalog_relations_where_clause_sql(relations) -%}
    where (
        {%- for relation in relations -%}
            {% if relation.schema and relation.identifier %}
                (
                    {{ snowflake__catalog_equals('table_schema', relation.schema) }}
                    and {{ snowflake__catalog_equals('table_name', relation.identifier) }}
                )
            {% elif relation.schema %}
                (
                    {{ snowflake__catalog_equals('table_schema', relation.schema) }}
                )
            {% else %}
                {% do exceptions.raise_compiler_error(
                    '`get_catalog_relations` requires a list of relations, each with a schema'
                ) %}
            {% endif %}

            {%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
    )
{%- endmacro %}
