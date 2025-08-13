{% macro is_incremental() %}
  {#-- do not run introspective queries in parsing #}
  {%- if not execute -%}
    {{ return(False) }}
  {%- else -%}
    {%- set relation = load_relation(this) -%}
    {{ return(relation is not none 
              and relation.type == 'table' 
              and not should_full_refresh()
              and (config.get('materialized') in ('incremental', 'incremental_scd2'))) }}
  {%- endif -%}
{%- endmacro -%}