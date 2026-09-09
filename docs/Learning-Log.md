# Learning Log

Design decisions, "why X over Y" reasoning, and things I learned while building — written as I go, not reconstructed afterward.

---

### Large vs Small semantic model storage format
**Decision:** Set workspace semantic model storage format to **Large**.

**Why:** Small format forces Power BI back into classic Import-mode behavior and disables Direct Lake entirely. Large format is a prerequisite for Direct Lake, which lets Power BI read Delta tables directly from OneLake — no data duplication, no scheduled refresh needed, and reports reflect gold-layer data as soon as it lands. Since Direct Lake is one of Fabric's core differentiators (and a DP-700 topic), I wanted the project to actually use it rather than default to old Import-mode habits from classic Power BI.

**Trade-off:** Direct Lake has some limitations (e.g. certain DAX/model features behave differently than Import mode) — something to watch for once the gold layer report is built.

---

### LastModifiedTS as the incremental watermark column
**Decision:** Use `LastModifiedTS` (not `OrderDate`) as the watermark column for incremental loads.

**Why:** `OrderDate` only reflects when an order was originally placed — it never changes. But orders get updated after creation (e.g. status moves from "Processing" to "Delivered"), and I need to capture those updates too, not just brand-new rows. `LastModifiedTS` updates whenever a record changes, so `WHERE LastModifiedTS > last_run_timestamp` correctly captures both new orders and updated orders in one query. Using `OrderDate` alone would have silently missed all the status updates on existing orders.

**Trade-off:** This assumes the source system reliably updates `LastModifiedTS` on every change — in a real production source, that's not always guaranteed, and is itself a data quality thing to validate.

---

### Synthetic dataset with separate historical + incremental files (instead of one static CSV)
**Decision:** Generate a 200K-row historical file plus three separate "daily incremental batch" files, rather than using a single static public dataset as-is.

**Why:** Most public datasets (Kaggle, etc.) are one static snapshot — they don't let you demonstrate an actual incremental pipeline because there's no "next day's data" to load. By generating historical + Day 1/2/3 batches myself, with each batch containing a mix of brand-new orders and updates to existing orders, I can build and prove a real MERGE/upsert pattern end-to-end, not just an append-only load.

**Trade-off:** Synthetic data means slightly less "this is real-world messy data" credibility than a genuine public dataset — worth being upfront about this if asked in an interview.

---

---

### SCD Type 2 for customer dimension, and a separate change-feed file (instead of Type 1 overwrite)
**Decision:** Track customer attributes (`City`, `Country`, `Email`) using SCD Type 2 in a dedicated `dim_customer` table, driven by a separate `customer_profile_changes.csv` change feed — rather than overwriting values in place (Type 1) or embedding profile changes into the orders data itself.

**Why:**
- **Type 2 over Type 1:** Type 1 loses history — if a customer's city changes, you can no longer tell what city they were in when a past order was placed. Type 2 preserves that context by keeping the old row (marked inactive) alongside a new active row. This is a much stronger signal of dimensional modeling understanding than Type 1, which barely requires MERGE logic at all.
- **Separate change-feed file over embedding in orders data:** SCD2 is a *dimension* concern, not a *fact* (transactional) concern — applying it to orders directly wouldn't make sense, since each order is already a one-time event. Modeling customer changes as their own feed also mirrors how real systems usually work (e.g. a CRM/master-data change feed is typically separate from an Orders/OMS feed), which makes the pipeline design more realistic and easier to explain.
- **Surrogate key (`CustomerSK`) alongside natural key (`CustomerID`):** once a customer can have multiple physical rows (one per version), the natural key alone can't uniquely identify a specific row anymore — the surrogate key does that, while `CustomerID` still ties all versions of the same customer together.

**Trade-off / open design question:** should `fact_orders` join to `dim_customer` on `CustomerID` (always shows current customer info) or on the `CustomerSK` that was active *at the time of the order* (shows historically accurate info)? The second is the "textbook correct" SCD2 pattern but adds complexity to the fact load (need to look up the correct active surrogate key at order time, not load time). Deciding this once the fact/dimension MERGE logic is actually being built.

---

### Dedicated `customer_master.csv` as the dim_customer source (instead of deriving it from orders data)
**Decision:** Generate a standalone `customer_master.csv` (5,000 customers) as the Day 0 baseline load for `dim_customer`, rather than deriving initial customer records by deduplicating the orders dataset.

**Why:** Deriving a dimension from transactional data conflates two different concerns — master data (who the customer is) and transactional data (what they ordered). Real systems almost always source customer dimensions from a dedicated master source (CRM/ERP extract), separate from order/transaction feeds. Having `customer_master.csv` as its own file makes the design match that real-world pattern, and makes the initial `dim_customer` load a clean, independent step rather than a derived/cleanup step buried inside the orders pipeline.

---

### 6-digit CustomerID instead of a simple sequential integer
**Decision:** Generate `CustomerID` as a random unique 6-digit number (e.g. `770487`) rather than a simple incrementing sequence (1, 2, 3…).

**Why:** Sequential integer IDs starting at 1 are an obvious tell of synthetic/toy data. Real-world systems almost never expose sequential IDs starting from 1 (often due to ID pooling, sharding, or simply years of accumulated records) — a 6-digit non-sequential ID looks and behaves like a realistic natural/business key, which matters when the goal is to demonstrate production-style data engineering, not just a tutorial dataset.

---

---

### Change feed as a full-row snapshot matching master's exact schema (no ChangeType/ChangeDate), including new customers
**Decision:** Give `customer_profile_changes.csv` the exact same schema and column names as `customer_master.csv` (`CustomerID, CustomerName, Email, City, Country, CustomerSince`) — no `ChangeType`/`ChangeDate` metadata — and include both updates to existing customers *and* entirely new customers in the same file.

**Why:**
- **Same column names as master:** the SCD2 MERGE logic reads both sources together (current master row vs. incoming row) — matching names avoid unnecessary aliasing before comparing/merging.
- **Full-row snapshot over a metadata-tagged diff:** many real-world source systems (CRMs, ERPs) don't emit a helpful "here's what changed and when" feed — they just expose the current state of a record. Modeling the change feed the same way is more realistic, and it means the pipeline has to do real work to figure out what changed, rather than being handed the answer.
- **INSERT vs. UPDATE detection becomes a pipeline responsibility:** since the feed doesn't flag whether a row is new or updated, the silver-layer MERGE logic has to check `CustomerID` against the current `dim_customer` itself — if it's not found, treat as a new customer (insert with `IsActive = 1`); if found, compare attributes and apply the expire-old/insert-new SCD2 pattern only if something actually changed. This is a more realistic (and more defensible in an interview) MERGE implementation than one that's handed an explicit `ChangeType` flag to key off.
- **No source-provided change timestamp:** without a `ChangeDate` column, the pipeline uses its own **load/batch date** as `EffectiveStartDate` when a change is detected. This is a genuine trade-off worth naming: it means `EffectiveStartDate` reflects *when the pipeline processed the change*, not necessarily the exact moment the change happened at the source — acceptable for a daily-batch pattern, but worth being explicit about if asked.

**Trade-off:** Detecting "did anything actually change" now requires comparing every attribute between the incoming row and the current master row (rather than trusting a `ChangeType` flag), which is slightly more MERGE logic to write — but it's the more realistic and more impressive pattern to demonstrate.

---

### Order status tracked as full history (fact_order_status_history), not overwritten in place
**Decision:** Track every stage an order passes through (`Ordered → Processing → Shipped → Delivered`, or `Cancelled`/`Returned`) as a separate history table — `fact_order_status_history` — using an SCD2-style expire-old/insert-new pattern, rather than simply overwriting `OrderStatus` on the order row each time it changes.

**Why:** Overwriting status in place (what the pipeline did initially) destroys information — once an order moves from "Processing" to "Delivered," there's no way to know it was ever "Processing," or when that transition happened. For a transparent, auditable pipeline, every stage needs to be preserved, not just the latest one. This also avoids ambiguity about "when was this order actually placed" vs. "when did it last change" — the full timeline is explicit and queryable.

**Design choice — separate history table, not full-row SCD2 on fact_orders:** Applying SCD2 to the *entire* order row (like we do for `dim_customer`) would be overkill, since most order attributes (customer, product, quantity, amount, order date) never change after placement — only status does. Splitting status into its own history table keeps `fact_orders` immutable and simple, while still fully preserving status lifecycle history where it's actually needed. This is the same underlying mechanic as SCD2 (expire old "current" row, insert new one) — just applied to a fact attribute instead of a dimension attribute, which is a valid and common pattern (sometimes called a fact table with "status tracking" or treated as a mini accumulating-snapshot pattern).

**Trade-off:** This does mean an extra table and an extra join when you want "orders with their current status" — worth it here for the transparency/audit benefit, but a reasonable engineer could also argue for a simpler accumulating-snapshot design (one row per order, with a column for each milestone's timestamp — `OrderedDate, ProcessingDate, ShippedDate, DeliveredDate`) as a lighter-weight alternative. Went with the history-table approach here since it more directly demonstrates the SCD2 pattern this project is meant to showcase.

---

*(More entries added as the project progresses — bronze/silver/gold design choices, pipeline orchestration decisions, monitoring approach, etc.)*
