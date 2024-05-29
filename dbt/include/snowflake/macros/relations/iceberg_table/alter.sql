{% macro snowflake__get_alter_iceberg_table_as_sql(
    existing_relation,
    configuration_changes,
    target_relation,
    sql
) -%}
    {{- log('Applying ALTER to: ' ~ existing_relation) -}}

    {% if configuration_changes.requires_full_refresh %}
        {{- get_replace_sql(existing_relation, target_relation, sql) -}}

    {% else %}

        {%- set catalog = configuration_changes.catalog -%}
        {%- if catalog -%}{{- log('Applying UPDATE CATALOG to: ' ~ existing_relation) -}}{%- endif -%}

        {% set external_volume = configuration_changes.external_volume -%}
        {%- if external_volume -%}{{- log('Applying UPDATE EXTERNAL VOLUME to: ' ~ existing_relation) -}}{%- endif -%}

        {%- set base_location = configuration_changes.base_location -%}
        {%- if base_location -%}{{- log('Applying UPDATE BASE LOCATION to: ' ~ existing_relation) -}}{%- endif -%}

        alter iceberg table {{ existing_relation }} set
            {% if catalog %}catalog = '{{ catalog.context }}'{% endif %}
            {% if external_volume %}external_volume = '{{ external_volume.context }}'{% endif %}
            {% if base_location %}base_location = '{{ base_location.context }}'{% endif %}

    {%- endif -%}

{%- endmacro %}
