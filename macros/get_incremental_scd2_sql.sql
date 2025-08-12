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
    {%- set change_type_col = arg_dict.get('change_type_column', var('change_type_column', '_CHANGE_TYPE')) -%}
    {%- set scd_check_columns = arg_dict.get('scd_check_columns', none) -%}
    {%- set change_type_expr = arg_dict.get('change_type_expr', none) -%}
    {%- set default_valid_to = arg_dict.get('default_valid_to', var('default_valid_to', '2999-12-31 23:59:59+0000')) -%}
    {%- set audit_cols_names = [is_current_col, valid_from_col, valid_to_col, updated_at_col, change_type_col] -%}

    {% set updated_at = '"' + updated_at_col + '"' %}

    {# Prepare column lists for the MERGE statement #}
    {%- set unique_keys_csv = dbt_snowflake_incremental_scd2.get_quoted_csv(unique_key | map("upper")) -%}
    {%- set dest_cols_names = dest_columns | map(attribute="name") | map("upper") | reject('in', audit_cols_names) | list -%}
    {%- set dest_cols_csv = dbt_snowflake_incremental_scd2.get_quoted_csv(dest_cols_names) -%}
    {%- set all_cols_names = dest_cols_names + audit_cols_names -%}
    {%- set all_cols_csv = dbt_snowflake_incremental_scd2.get_quoted_csv(all_cols_names) -%}

    {# Process change_type_expr - defaults to change_type_col if not provided #}
    {%- if change_type_expr -%}
        {%- set change_type_sql = change_type_expr -%}
    {%- else -%}
        {# Default ROW_NUMBER logic #}
        {%- set change_type_sql = "CASE WHEN ROW_NUMBER() OVER (PARTITION BY " + unique_keys_csv + " ORDER BY " + updated_at + ") = 1 THEN 'I' ELSE 'U' END" -%}
    {%- endif -%}

    {# Build hash-based change detection #}
    {%- if scd_check_columns -%}
        {%- set hash_columns = scd_check_columns -%}
    {%- else -%}
        {# If no scd_check_columns specified, use all business columns #}
        {%- set hash_columns = dest_cols_names -%}
    {%- endif -%}

{# This section is where the magic happens: the MERGE statement #}
merge into {{ target_relation }} AS DBT_INTERNAL_DEST
using (
    with
        {# New records are those coming from our current run, based on the model logic and run mode. #}
        new_records as (
            select
                {{ dest_cols_csv }},
                {{ updated_at }},
                {{ change_type_sql }} as {{ change_type_col }},
                {{ dbt_utils.generate_surrogate_key(hash_columns | list) }} as _scd2_hash
            from {{ temp_relation }}
        ),
        {# We need the existing version of any records that are about to be updated #}
        previous_record as (
            select
                {{ dest_cols_csv }},
                {{ updated_at }},
                {{ change_type_col }},
                {{ dbt_utils.generate_surrogate_key(hash_columns | list) }} as _scd2_hash
            from {{ this }}
            where {{ is_current_col }}
                and {{ unique_keys_csv }} in (select {{ unique_keys_csv }} from new_records)
        ),
        {# Bring the band together - only include records where columns have changed #}
        all_records as (
            select 
                {% for col in dest_cols_names %}
                n.{{ col }},
                {% endfor %}
                n.{{ updated_at }},
                n.{{ change_type_col }}
            from new_records n
            left join previous_record p on n.{{ unique_keys_csv }} = p.{{ unique_keys_csv }}
            where p.{{ unique_keys_csv.split(',')[0] }} is null 
               or n._scd2_hash != p._scd2_hash
            
            union all
            
            select 
                {% for col in dest_cols_names %}
                p.{{ col }},
                {% endfor %}
                p.{{ updated_at }},
                'U' as {{ change_type_col }}
            from previous_record p
            inner join new_records n on p.{{ unique_keys_csv }} = n.{{ unique_keys_csv }}
            where n._scd2_hash != p._scd2_hash
        )
    select
        {{ dest_cols_csv }},
        {# SCD2 audit columns using reusable macros #}
        {{ dbt_snowflake_incremental_scd2.get_is_current_sql(unique_keys_csv, updated_at) }} as {{ is_current_col }},
        {{ dbt_snowflake_incremental_scd2.get_valid_from_sql(updated_at) }} as {{ valid_from_col }},
        {{ dbt_snowflake_incremental_scd2.get_valid_to_sql(unique_keys_csv, updated_at, default_valid_to) }} as {{ valid_to_col }},
        {{ updated_at }} as {{ updated_at_col }},
        {{ change_type_col }}
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