{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if custom_alias_name is none -%}

        ns__{{ env_var('DB_NAMESPACE', '') }}__ns__{{ project_name }}_{{ node.name }}

    {%- else -%}

        ns__{{ env_var('DB_NAMESPACE', '') }}__ns__{{ custom_alias_name | trim }}

    {%- endif -%}

{%- endmacro %}
