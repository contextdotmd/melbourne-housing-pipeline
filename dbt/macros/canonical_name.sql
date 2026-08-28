{#
  Pick one display spelling per key: the most frequent, ties broken alphabetically.

  The source spells some suburbs and agencies more than one way. Keys are lowercased so the
  variants collapse to one dimension member, which then needs a single display form. Choosing
  by frequency gives the spelling a reader expects; the alphabetical tie-break makes the
  choice deterministic, so the dimension does not change between rebuilds for no reason.
#}
{% macro canonical_name(relation, key_column, name_column) %}
    select
        {{ key_column }},
        {{ name_column }}
    from (
        select
            {{ key_column }},
            {{ name_column }},
            row_number() over (
                partition by {{ key_column }}
                order by count(*) desc, {{ name_column }}
            ) as _rn
        from {{ relation }}
        group by {{ key_column }}, {{ name_column }}
    )
    where _rn = 1
{% endmacro %}
