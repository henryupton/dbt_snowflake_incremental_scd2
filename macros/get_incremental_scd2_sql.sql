{% macro get_incremental_scd2_sql(arg_dict) %}
    {% set target_relation = arg_dict["target_relation"] %}
    {% set temp_relation = arg_dict["temp_relation"] %}
    {% set unique_key = arg_dict["unique_key"] %}
    {% set dest_columns = arg_dict["dest_columns"] %}
    {% set incremental_predicates = arg_dict.get("incremental_predicates", []) %}

    {# Define our audit columns – these are crucial for SCD2 tracking. These need to be present in the table already too.#}
    {%- set is_current_col = arg_dict.get('is_current_column', var('is_current_column', '_IS_CURRENT')) -%}
    {%- set valid_from_col = arg_dict.get('valid_from_column', var('valid_from_column', '_VALID_FROM')) -%}
    {%- set valid_to_col = arg_dict.get('valid_to_column', var('valid_to_column', '_VALID_TO')) -%}
    {%- set updated_at_col = arg_dict.get('updated_at_column', var('updated_at_column', '_UPDATED_AT')) -%}
    {%- set scd_hash_col = arg_dict.get('scd_hash_column', var('scd_hash_column', '_SCD_HASH')) -%}
    {%- set scd_check_columns = arg_dict.get('scd_check_columns', none) -%}
    {%- set default_valid_to = arg_dict.get('default_valid_to', var('default_valid_to', '2999-12-31 23:59:59+0000')) -%}
    {%- set audit_cols_names = [is_current_col, valid_from_col, valid_to_col, updated_at_col, scd_hash_col] -%}

    {% set updated_at = '"' + updated_at_col + '"' %}

    {# Prepare column lists for the MERGE statement #}
    {%- set unique_keys_csv = get_quoted_csv(unique_key | map("upper")) -%}
    {%- set dest_cols_names = dest_columns | map(attribute="name") | map("upper") | reject('in', audit_cols_names) | list -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_cols_names) -%}
    {%- set all_cols_names = dest_cols_names + audit_cols_names -%}
    {%- set all_cols_csv = get_quoted_csv(all_cols_names) -%}

{# This section is where the magic happens: the MERGE statement #}
merge into {{ target_relation }} AS DBT_INTERNAL_DEST
using (
    with
        {# New records are those coming from our current run, based on the model logic and run mode. #}
        new_records as (
            select
                {{ dest_cols_csv }},
                {{ updated_at }},
                {{ generate_scd_hash(temp_relation, scd_check_columns, audit_cols_names) }} as {{ scd_hash_col }}
            from {{ temp_relation }}
        ),
        {# We need the existing version of any records that are about to be updated #}
        previous_record as (
            select
                {{ dest_cols_csv }},
                {{ updated_at }},
                {{ scd_hash_col }}
            from {{ this }}
            where {{ is_current_col }}
                and {{ unique_keys_csv }} in (select {{ unique_keys_csv }} from new_records)
        ),
        {# Bring the band together - only include records where hash has changed #}
        all_records as (
            select 
                n.*,
                case 
                    when p.{{ scd_hash_col }} is null then 'new'
                    when n.{{ scd_hash_col }} != p.{{ scd_hash_col }} then 'changed'  
                    else 'unchanged'
                end as _change_type
            from new_records n
            left join previous_record p on n.{{ unique_keys_csv }} = p.{{ unique_keys_csv }}
            where p.{{ scd_hash_col }} is null 
               or n.{{ scd_hash_col }} != p.{{ scd_hash_col }}
            
            union all
            
            select 
                p.*,
                'expire' as _change_type
            from previous_record p
            inner join new_records n on p.{{ unique_keys_csv }} = n.{{ unique_keys_csv }}
            where n.{{ scd_hash_col }} != p.{{ scd_hash_col }}
        )
    select
        {{ dest_cols_csv }},
        {# SCD2 audit columns using reusable macros #}
        {{ get_is_current_sql(unique_keys_csv, updated_at) }} as {{ is_current_col }},
        {{ get_valid_from_sql(updated_at) }} as {{ valid_from_col }},
        {{ get_valid_to_sql(unique_keys_csv, updated_at, default_valid_to) }} as {{ valid_to_col }},
        {{ updated_at }} as {{ updated_at_col }},
        {{ scd_hash_col }}
    from all_records
    ) AS DBT_INTERNAL_SOURCE
on (
    {# Matching condition for the MERGE: unique key and the updated_at timestamp #}
    {% for col in unique_key %}
        DBT_INTERNAL_DEST.{{ col }} = DBT_INTERNAL_SOURCE.{{ col }}{% if not loop.last %},{% endif %}
    {%- endfor %}
    and DBT_INTERNAL_DEST.{{ updated_at }} = DBT_INTERNAL_SOURCE.{{ updated_at }}
    {%- if incremental_predicates -%}
    {# Optional: Incremental Predicates (if defined in dbt_project.yml or model config) #}
    and (
        {% for predicate in incremental_predicates %}
            {{ predicate }}
            {% if not loop.last %} AND {% endif %}
        {% endfor %}
        )
    {%- endif -%}
)
{# When a match is found, we update the existing record (this typically happens to set _is_current to false or _valid_to for old records) #}
when matched then update set
    {% for col in all_cols_names %}
        DBT_INTERNAL_DEST.{{ col }} = DBT_INTERNAL_SOURCE.{{ col }}{% if not loop.last %},{% endif %}
    {%- endfor %}
{# When no match is found, it's a new record or a new version of an existing record, so we insert it #}
when not matched then insert ({{ all_cols_csv }})
VALUES ({{ all_cols_csv }})
{% endmacro %}
