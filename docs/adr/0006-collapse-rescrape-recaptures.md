---
status: accepted
---

# Collapse duplicated rows under three named reasons, quarantine the evidence

860 groups of rows are identical on every column except `Date`; 786 of them span exactly five
dates — 16/12/2017, 23/12/2017, 30/12/2017, 6/01/2018 and 8/01/2018 — each carrying exactly 787
rows, 100% of which sit in a duplicate group. Inter-row gaps cluster at 7 days (2,395) and 2
days (787). The source stopped updating over the Melbourne Christmas auction shutdown and
republished the 16/12 snapshot four more times, stamped with the capture date. Left in, this
inflates December–January volume roughly fivefold and fabricates thousands of resale pairs with
zero price change.

Rows identical on all business columns except `Date`, and separated by 21 days or less, are
collapsed to their earliest date. The removed rows are written to
`quarantine_recaptured_listing` with reason `suspected_recapture` rather than deleted.

## A second, opposite kind of duplication

The recapture rule is blind to *restatements*: the same property, date, method and agent with
changed content — almost always a price published after the fact, so a sale recorded once as
"not disclosed" and again with a figure. A recapture holds content constant and varies the
date; a restatement does the exact opposite. No single signature catches both, because one
containing the price cannot see restatements and one containing the date cannot see recaptures.

Restatements are therefore resolved first, keeping the most informative row (a published
figure beats a null), and excluded from the recapture chain — a duplicate on day one would
otherwise become the lag anchor for the following week and the gap would be measured from the
wrong row.

Exact duplicates — the same row twice, nothing changed — are named separately rather than
folded into restatements, because nothing was restated.

Quarantine reasons and their counts: `suspected_recapture` 3,200, `suspected_restatement` 5,
`exact_duplicate` 2.

## Consequences

The 21-day window is a judgement call. It was chosen because observed gaps cluster at 2 and 7
days, the next is 14, and the one after that is 49 — so the boundary sits in empty space and
preserves roughly ten groups that are identical-except-date but 49–203 days apart, which are
plausible genuine relistings.

Note that any dedup key *including* `Date` is structurally blind to this artefact, because
`Date` is precisely what varies. Quarantining rather than deleting keeps the decision provable
and lets anyone who disputes the window see exactly which rows it affected.
