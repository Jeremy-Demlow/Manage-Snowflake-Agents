{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Standard dbt macro override for clean schema names.

        Default dbt behavior: prepends target.schema to custom_schema_name
        - schema='staging' → MARTS_STAGING (ugly!)

        This override: uses custom_schema_name exactly as specified
        - schema='staging' → STAGING (clean!)

        Reference: https://docs.getdbt.com/docs/build/custom-schemas
    #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim | upper }}
    {%- endif -%}
{%- endmacro %}
