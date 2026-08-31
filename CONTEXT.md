# Melbourne Housing Market

The language of Melbourne residential property auctions, as recorded by
`MELBOURNE_HOUSE_PRICES_LESS.csv`. This context exists because the source file's column names
are misleading: `Price` means two different things depending on the row, and `Method` encodes
whether a sale happened at all. Getting the vocabulary right is what stops the wrong number
being computed.

## Language

### Outcomes

**Listing Outcome**:
The result of one marketing or auction campaign for one **Property** on one event date.
_Avoid_: sale (a Listing Outcome is only sometimes a Sale), listing, record, transaction

**Sale**:
A **Listing Outcome** in which the property changed hands — `Method` ∈ {S, SP, SA, SN, PN, SS}.
_Avoid_: sold record, settlement, deal

**Passed In**:
A **Listing Outcome** where bidding failed to reach the vendor's reserve — `Method` = PI.
_Avoid_: failed sale, no sale

**Vendor Bid**:
A **Listing Outcome** where the auctioneer bid on the vendor's behalf and the property did not
sell — `Method` = VB.
_Avoid_: dummy bid, reserve bid

**Withdrawn**:
A **Listing Outcome** pulled from market before auction — `Method` = W.
_Avoid_: cancelled, expired

### Money

**Sale Price**:
The consideration paid, present only when a **Listing Outcome** is a **Sale** and the amount
was disclosed.
_Avoid_: price, value, amount

**Bid Amount**:
The highest or vendor bid recorded against a **Listing Outcome** that was *not* a **Sale**.
_Avoid_: price, offer, reserve

**Disclosed Price**:
Whether an amount was published for a **Listing Outcome**. A row-level fact, not a property of
the `Method`.
_Avoid_: has price, price known

### Data quality

**Restatement**:
A later correction of an already-recorded **Listing Outcome** — same property, date, method
and agent, but changed content, almost always a price published after the fact.
_Avoid_: update, revision, duplicate

**Recapture**:
A republished snapshot of an earlier **Listing Outcome**, stamped with the capture date rather
than the event date. Not a distinct event.
_Avoid_: duplicate (an exact duplicate is a different thing), re-run, reload

**Quarantine**:
The store of rows deliberately excluded from the fact, each carrying the reason for exclusion.
_Avoid_: rejects (that term is reserved for rows the loader could not parse), errors, bad data

### Geography and identity

**Property**:
A dwelling, identified by **Suburb** plus street address. Address alone is not unique —
"5 Charles St" occurs in seven suburbs, "14 Moray St" in three.
_Avoid_: house, address, dwelling

**Suburb**:
The geographic grain of this dataset. Determines postcode, region, council area, **CBD
Distance** and **Property Count** — all five hold with zero exceptions across the 377 suburbs.
_Avoid_: location, area, locality

**CBD Distance**:
Kilometres from the Melbourne central business district. An attribute of a **Suburb**.
_Avoid_: distance (ambiguous), proximity

**Property Count**:
The number of dwellings in a **Suburb**. An attribute of the **Suburb**, never a measure of
market activity.
_Avoid_: count, volume, sales count

### Metrics

**Clearance Rate**:
**Sales** divided by all **Listing Outcomes** over a period. The headline measure of Melbourne
market strength.
_Avoid_: sell-through, success rate, conversion

**Auction Saturday**:
The Saturday on which the overwhelming majority of campaigns conclude — 91% of all **Listing
Outcomes**.
_Avoid_: sale date, auction day

## Relationships

- A **Property** has one or more **Listing Outcomes** over time
- A **Listing Outcome** belongs to exactly one **Property**, one **Suburb** and one agent
- A **Listing Outcome** is either a **Sale** or not; **Passed In**, **Vendor Bid** and
  **Withdrawn** are the three non-Sale forms
- A **Sale** has a **Sale Price** only when its price was disclosed — 8,965 Sales in the
  fact have none (9,324 source rows before deduplication)
- A non-Sale may still carry a **Bid Amount** — 10,964 do
- A **Recapture** references an earlier **Listing Outcome**; only the earliest survives into
  the fact, the rest go to **Quarantine**
- **Clearance Rate** is computed over **Listing Outcomes**, never over **Sales** alone —
  dividing Sales by Sales is always 1

## Example dialogue

> **Dev:** "For the suburb monthly view I'm averaging `Price` — that gives me average sale
> price, right?"
>
> **Domain expert:** "No. About eleven thousand of those rows were **Passed In** or a **Vendor
> Bid** — the property didn't sell. The number recorded is the highest bid, not a **Sale
> Price**. Average them together and you're mixing what buyers offered with what properties
> actually fetched."
>
> **Dev:** "So I filter to sold rows and average from there?"
>
> **Domain expert:** "Closer. But a third of **Sales** have no price at all — sold prior, sold
> not disclosed. Those are still **Sales**, they just have no **Disclosed Price**. If you drop
> them you'll understate volume; if you count them in the average you'll get nulls. And don't
> drop them from the **Clearance Rate** — a Sale with an undisclosed price still cleared."
>
> **Dev:** "What about the same property appearing five Saturdays running at the same price?"
>
> **Domain expert:** "That's not five campaigns, that's one. The feed stopped updating over
> the Christmas shutdown and kept republishing the same snapshot. Those are **Recaptures** —
> keep the earliest, quarantine the rest, or your December volume is five times reality."

## Flagged ambiguities

- **"sale"** was used for both *any row in the file* and *a property that actually changed
  hands* — resolved: **Listing Outcome** is the row, **Sale** is the subset that transacted.
  The fact table is named for the former precisely so nobody assumes the latter.
- **"price"** was used for both consideration paid and highest bid on an unsold property —
  resolved: **Sale Price** and **Bid Amount** are separate, mutually exclusive columns. There
  is deliberately no column called `price`.
- **"duplicate"** was used for three different things — resolved into three named reasons,
  because they have opposite signatures and no single rule catches them all:
  **Recapture** (3,200: same content, *different* date), **Restatement** (5: same date,
  *different* content), and plain **exact duplicate** (2: the same row twice, nothing changed).
- **Agency identity is not just a casing problem.** `C21` and `Century` are the same agency
  recorded two ways, and lowercasing cannot merge them. The un-merged `Century` member carries
  37 outcomes against `C21`'s 309. No alias list is maintained: inventing one from 470 names
  would guess more than it resolved.
- **A compound agency name is a co-listing.** 42 agency names contain a slash
  (`Buxton/Hodges`, `Fletchers/Fletchers`), covering 126 outcomes. These are two agencies
  sharing one campaign, encoded in one string — so co-listings are recorded, just not
  decomposed. Modelled as a single agency, since splitting them would need a bridge table for
  126 rows.
- **"missing price"** was used for two unrelated situations — a property that didn't sell, and
  a sale whose price wasn't published — resolved: these are distinguished by **Sale** vs
  non-Sale, with **Disclosed Price** covering the second.
- **"rejects"** vs **"quarantine"** — resolved: **rejects** are rows the loader could not
  parse (a contract failure); **Quarantine** holds rows that parsed cleanly but were excluded
  by a modelling rule.
- **Counting spellings vs counting entities** — the source holds 380 suburb spellings and 476
  agent spellings, but case variants (`Croydon`/`croydon`, `MacLeod`/`Macleod`,
  `Viewbank`/`viewbank`; `VICPROP`/`VICProp`/`Vicprop`, `LITTLE`/`Little`) mean there are
  **377 Suburbs** and **470 Agents**. Resolved: dimensions key on the lowercased value and
  choose the display spelling by frequency. Left unnormalised each variant becomes its own
  dimension member, quietly splitting that suburb's or agent's numbers in two.
- **"property characteristics"** — `Rooms` and `Type` read like attributes of a **Property**,
  but they differ between outcomes for 110 and 71 properties respectively. Resolved: they are
  recorded as at the **Listing Outcome**, not as facts about the dwelling. See
  [docs/data-model.md](docs/data-model.md).
