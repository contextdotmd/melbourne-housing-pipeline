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
