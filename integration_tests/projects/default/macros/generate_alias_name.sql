{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if custom_alias_name is none -%}

        ns_{{ env_var('DB_NAMESPACE', '') }}_{{ project_name }}_{{ node.name }}

    {%- else -%}

        ns_{{ env_var('DB_NAMESPACE', '') }}_{{ custom_alias_name | trim }}

    {%- endif -%}

{%- endmacro %}
