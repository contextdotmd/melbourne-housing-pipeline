{#
  Shared definition of "what this run has to reprocess".

  The orchestrator knows which load ids it just ingested and passes them in:

      dbt build --vars '{"load_ids": ["a1b2c3", "d4e5f6"]}'

  Every incremental model derives its scope from that one list, so the whole warehouse agrees
  on what changed. With no var supplied — a manual run, a full refresh, CI — the scope is
  everything, and each model degrades to its full-refresh behaviour. That is deliberate: the
  incremental path must never be the only way to get a correct answer.
#}

{% macro incoming_load_ids() %}
    {{ return(var('load_ids', none)) }}
{% endmacro %}


{% macro is_scoped_run() %}
    {#- true only when this is an incremental build AND the caller named the loads -#}
    {{ return(is_incremental() and var('load_ids', none)) }}
{% endmacro %}


{% macro load_id_list() %}
    {%- set ids = var('load_ids', []) -%}
    {%- for id in ids -%}'{{ id }}'{% if not loop.last %}, {% endif %}{%- endfor -%}
{% endmacro %}


{% macro affected_properties() %}
{#-
  Properties touched by the incoming loads.

  Property is the correct unit because BOTH deduplication rules partition by a key that
  contains it — restatement by (property, date, method, agent), recapture by a signature that
  begins with the property. No dedup group can therefore span two properties, so reprocessing
  a property in isolation is always complete. Verified by
  assert_no_dedup_group_spans_two_properties.sql.
-#}
    select distinct
        suburb_key,
        street_address_key
    from {{ ref('stg__listing_outcome') }}
    where load_id in ({{ load_id_list() }})
{% endmacro %}


{% macro affected_property_keys() %}
{#-
  The same scope as affected_properties(), expressed as the surrogate key. Models downstream
  of the fact carry property_key rather than the columns it hashes, so the key is derived here
  with the identical expression dim_property and the fact use — one definition, three callers.
-#}
    select {{ dbt_utils.generate_surrogate_key(['suburb_key', 'street_address_key']) }}
               as property_key
    from ({{ affected_properties() }}) as scope
{% endmacro %}


{% macro overwrite_affected_partitions(partitioned_by='property_key') %}
{#-
  Overwrite the partitions this run touches.

  The unit is the PROPERTY, and that is a partition, not a unique key. A fact table having
  many rows per property is not a defect — it is what a many-to-one dimension foreign key
  means, and 1,092 properties genuinely have more than one listing outcome. Each model's real
  unique key is asserted separately and normally: listing_outcome_key on the fact,
  (load_id, source_row) on staging and the clustered model, property_key on dim_property.

  dbt's delete+insert strategy would do the same thing, and its `unique_key` is documented as
  tolerating a non-unique column. But the reprocessing unit here is the PROPERTY, and a
  property has many rows — 59,816 outcomes across 58,696 properties, one of them with four.
  Declaring that as `unique_key` asserts something untrue about the model, and the next reader
  will either believe it or switch to `merge` — which cannot express this operation at all.
  When a later delivery makes an existing outcome a recapture, that row must STOP EXISTING,
  and merge has no way to delete what no longer qualifies.

  Each model keeps a real uniqueness test on its actual row key, asserted the normal way.

  This is `insert_overwrite` semantics — replace whole partitions — emulated because
  dbt-duckdb has no such strategy. On BigQuery or Spark it would be the native
  `insert_overwrite` with `partition_by`, and on Snowflake the same delete-then-insert. The
  operation is identical; only the syntax differs.

  `partitioned_by` selects how the partition is expressed: 'property_key' for models carrying
  the hash, 'property_parts' for those carrying the columns it hashes.
-#}
    {%- if not is_incremental() -%}
        {#- First build or --full-refresh: the table is being created, nothing to remove. -#}
        select 1 where false
    {%- elif var('load_ids', none) -%}
        {#- Scoped run: replace only the partitions the incoming loads touch. -#}
        {%- if partitioned_by == 'property_parts' -%}
            delete from {{ this }}
            where (suburb_key, street_address_key) in (
                select suburb_key, street_address_key from ({{ affected_properties() }}) as scope
            )
        {%- else -%}
            delete from {{ this }}
            where property_key in ({{ affected_property_keys() }})
        {%- endif -%}
    {%- else -%}
        {#-
          Incremental table, but the caller named no loads - a manual `dbt build`, or CI.
          The model body then selects the whole history, so every partition is in scope and
          the table is replaced wholesale. Without this the append would simply double it.
        -#}
        delete from {{ this }}
    {%- endif -%}
{% endmacro %}
