{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set schema = custom_schema_name if custom_schema_name is not none else target.schema -%}
    {%- if target.name == 'ci' -%}
        ci_{{ schema | trim }}
    {%- else -%}
        {{ schema | trim }}
    {%- endif -%}
{%- endmacro %}