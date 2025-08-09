{% materialization incremental_scd2, default %}

  {%- set target_relation = this -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set tmp_relation = make_temp_relation(target_relation) -%}
  {%- set tmp_relation = tmp_relation.incorporate(type='table') -%}

  {# Get configurable audit column names #}
  {%- set is_current_col = config.get('is_current_column', var('is_current_column', '_IS_CURRENT')) -%}
  {%- set valid_from_col = config.get('valid_from_column', var('valid_from_column', '_VALID_FROM')) -%}
  {%- set valid_to_col = config.get('valid_to_column', var('valid_to_column', '_VALID_TO')) -%}
  {%- set updated_at_col = config.get('updated_at_column', var('updated_at_column', '_UPDATED_AT')) -%}
  {%- set loaded_at_col = config.get('loaded_at_column', var('loaded_at_column', '_LOADED_AT')) -%}
  {%- set scd_hash_col = config.get('scd_hash_column', var('scd_hash_column', '_SCD_HASH')) -%}
  {%- set scd_check_columns = config.get('scd_check_columns', none) -%}
  {%- set default_valid_to = config.get('default_valid_to', var('default_valid_to')) -%}
  {%- set unique_key = config.get('unique_key') -%}

  {%- if unique_key is none -%}
    {%- set error_message -%}
      You must provide a unique_key configuration for incremental_scd2 materialization.
      This should be the business key (natural key) of the dimension.
    {%- endset -%}
    {{ exceptions.raise_compiler_error(error_message) }}
  {%- endif -%}

  {{ log("Building SCD Type 2 table " ~ target_relation) }}

  {# Build the temp table with source data #}
  {%- call statement('build_temp_table') -%}
    {{ create_table_as(True, tmp_relation, sql) }}
  {%- endcall -%}

  {%- set should_full_refresh = (should_full_refresh() or existing_relation is none) -%}

  {%- if should_full_refresh -%}
    
    {# Initial load: create table with audit columns #}
    {{ log("Performing initial load for SCD2 table") }}
    
    {# Get audit column names for hash generation #}
    {%- set audit_columns = [is_current_col, valid_from_col, valid_to_col, updated_at_col, loaded_at_col, scd_hash_col] -%}
    
    {# Build the argument dictionary for the initial load SQL macro #}
    {%- set initial_load_arg_dict = {
      'temp_relation': tmp_relation,
      'unique_key': unique_key,
      'scd_check_columns': scd_check_columns,
      'audit_columns': audit_columns,
      'is_current_column': is_current_col,
      'valid_from_column': valid_from_col,
      'valid_to_column': valid_to_col,
      'updated_at_column': updated_at_col,
      'loaded_at_column': loaded_at_col,
      'scd_hash_column': scd_hash_col,
      'default_valid_to': default_valid_to
    } -%}
    
    {%- set initial_load_sql = get_initial_load_scd2_sql(initial_load_arg_dict) -%}

    {%- set build_sql = get_create_table_as_sql(False, target_relation, initial_load_sql) -%}

  {%- else -%}
    
    {# Incremental load: use SCD2 merge logic #}
    {{ log("Performing incremental SCD2 update") }}

    {%- set dest_columns = adapter.get_columns_in_relation(existing_relation) -%}
    
    {# Build the argument dictionary for the SCD2 SQL macro #}
    {%- set arg_dict = {
      'target_relation': target_relation,
      'temp_relation': tmp_relation,
      'unique_key': unique_key,
      'dest_columns': dest_columns,
      'incremental_predicates': config.get('incremental_predicates', []),
      'is_current_column': is_current_col,
      'valid_from_column': valid_from_col,
      'valid_to_column': valid_to_col,
      'updated_at_column': updated_at_col,
      'loaded_at_column': loaded_at_col,
      'scd_hash_column': scd_hash_col,
      'scd_check_columns': scd_check_columns,
      'default_valid_to': default_valid_to
    } -%}

    {%- set build_sql = get_incremental_scd2_sql(arg_dict) -%}

  {%- endif -%}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {%- if tmp_relation is not none -%}
    {%- do adapter.drop_relation(tmp_relation) -%}
  {%- endif -%}

  {%- set target_relation = target_relation.incorporate(type='table') -%}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
