-- The row ledger. Every row the loader accepted must end up in exactly one of two places:
-- the fact, or the quarantine. This is the assertion that makes "nothing was silently lost"
-- a fact rather than a hope, and it is why staging is deliberately row-preserving.

with ledger as (

    select
        (select count(*) from {{ ref('stg__listing_outcome') }})            as staged,
        (select count(*) from {{ ref('fact__listing_outcome') }})           as in_fact,
        (select count(*) from {{ ref('quarantine_recaptured_listing') }})   as quarantined

)

select *
from ledger
where in_fact + quarantined != staged
