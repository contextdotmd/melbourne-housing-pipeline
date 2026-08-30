{{ config(
    materialized = 'incremental',
    unique_key = 'property_key',
    incremental_strategy = 'delete+insert',
) }}

{#- dbt cannot infer a ref() that only appears inside a conditional, so the scope
    macro's dependency on staging is declared explicitly. -#}
-- depends_on: {{ ref('stg__listing_outcome') }}

-- One row per dwelling: identity only.
--
-- Keyed on suburb AND address because address alone is not unique — "14 Moray St" occurs in
-- seven suburbs, and merging them would splice unrelated sale histories together.
--
-- No rooms or type here: both differ between outcomes for the same dwelling (112 and 72
-- properties respectively), so they belong on the fact as at-the-outcome attributes.

{% if is_scoped_run() %}
with scoped as (

    select history.*
    from {{ ref('int__listing_outcome_deduped') }} as history
    inner join ({{ affected_properties() }}) as affected
        using (suburb_key, street_address_key)

),
{% else %}
with scoped as (

    select * from {{ ref('int__listing_outcome_deduped') }}

),
{% endif %}

canonical as (

    {{ canonical_name(
        'scoped',
        'suburb_key, street_address_key',
        'street_address'
    ) }}

)

select
    {{ dbt_utils.generate_surrogate_key(['suburb_key', 'street_address_key']) }} as property_key,
    suburb_key,
    street_address_key,
    street_address
from canonical
