{#
  Use the configured schema verbatim rather than dbt's default
  "<target_schema>_<custom_schema>" concatenation, so the warehouse reads
  staging / intermediate / marts / analytics rather than main_staging etc.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
