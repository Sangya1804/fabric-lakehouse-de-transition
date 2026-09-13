# Learning Log

Key design decisions and reasoning — kept tight and curated, not a running transcript of every conversation. Each entry: what was decided, why, and the honest trade-off.

---

### SCD Type 2 for dim_customer (not Type 1)  
Type 1 (overwrite in place) loses history — you can't tell what city a customer was in when a past order was placed. Type 2 preserves that by keeping the old row (marked inactive) alongside a new active row. Trade-off: more storage, more complex queries (need IsCurrentRecord filters), but it's the correct pattern when historical accuracy matters.

---

### LastModifiedTS as the incremental watermark (not OrderDate)  
OrderDate never changes after creation, so it can't detect updates to existing records — only OrderDate-based filtering would miss every status change. LastModifiedTS updates whenever a record changes, so WHERE LastModifiedTS > last_run_timestamp correctly captures both new and updated records in one query.

---

### Customer master + change feed as separate files, not derived from orders  
Real systems source customer dimensions from a dedicated master (CRM/ERP), separate from transactional data. Deriving dim_customer by deduplicating order records would conflate master data with transactional data — a customer_master.csv Day-0 baseline plus a customer_profile_changes.csv change feed matches how real pipelines are actually fed.

---

### Change feed as a full-row snapshot, not a metadata-tagged diff  
customer_profile_changes.csv has the exact same schema as master (no ChangeType/ChangeDate columns) and includes both updates and brand-new customers. This mirrors how real source extracts usually work — the source just sends current-state rows; the pipeline determines INSERT vs. UPDATE itself by comparing against the current dim_customer. More realistic, and a stronger MERGE implementation to demonstrate than one handed an explicit flag to key off.

---

### Orders table normalized — no customer attributes stored redundantly  
Originally, CustomerName/Email/City/Country were duplicated into every order row. This is a classic denormalization mistake: it can go stale (a customer's old orders would still show their old city after they move) and it violates star-schema design (facts hold foreign keys + measures, not descriptive attributes that belong to a dimension). Fixed to CustomerID-only, joined to dim_customer when needed.

---

### fact_order_status_history kept separate from fact_orders, not merged  
Tempting to just apply SCD2 to the whole fact_orders row for status changes — but that duplicates immutable measures (Quantity, UnitPrice, etc.) on every status change, and any query that forgets to filter IsCurrentRecord = 1 silently double- or triple-counts revenue. Splitting status into its own narrow table keeps fact_orders at a clean, unambiguous grain (one row per order line item, permanently) while still fully preserving status history where it's actually needed.

---

### Grain corrected twice — both times by testing against real-world behavior  
1. Orders → line-item grain: originally one row per order with a single product; real checkouts have multiple items per order, so fact_orders' grain became OrderID + LineNumber, not OrderID alone.
2. Status → line-item grain, not order grain: originally assumed a whole order shares one status. Wrong — Amazon/Flipkart-style platforms let individual items be cancelled or shipped independently. fact_order_status_history was corrected to key on OrderID + LineNumber, matching fact_orders.

Both mistakes had the same root cause: not stress-testing the grain against a realistic scenario before building on top of it. Worth naming directly in an interview — it's a stronger story than pretending the design was right the first time.

---

### Naming/formatting conventions standardized  
1. IsCurrentRecord used consistently for the "current version" flag across every SCD2-style table (not IsActive in one, IsCurrentStatus in another).
2. 9999-12-31 sentinel used instead of NULL for open-ended EffectiveEndDate/StatusEffectiveEndDate — point-in-time range queries (WHERE StartDate <= @d AND EndDate >= @d) work uniformly this way; NULL breaks that comparison and forces special-casing everywhere.
3. 6-digit non-sequential CustomerID instead of 1, 2, 3… — reads as a realistic business key rather than an obvious synthetic-data tell.

---

### dim_date generated via Spark sequence(), not a recursive CTE  
Recursive CTEs are the standard T-SQL technique for generating a date series — but Spark SQL doesn't support recursion at all. It doesn't need to: sequence(start, end, interval 1 day) + explode() generates the full date range directly in one call. A genuine platform difference, not just a stylistic choice.

---

### Raw/Bronze and Silver switched to truncate & load; only Gold persists history  
Originally, bronze was designed to accumulate history via appends. Reconsidered to match how real source systems actually behave — they hand over "today's file" with no history-tracking of their own, so raw/silver should be transient staging, truncated and reloaded each run. Gold becomes the only layer that persists accumulated history, via incremental MERGE (Type 1/2). This mirrors a classic SSIS staging-table pattern (staging → cleaned staging → MERGE into dimension/fact), just implemented with Fabric notebooks and Delta MERGE.

__Follow-up:__ truncate & load means the raw/silver tables retain no audit trail of what a source file looked like on a given day. Fixed by archiving each source file itself — after a successful raw load, the file is moved from Files/incoming/ to Files/archive/<date>/ (mssparkutils.fs.mv()), rather than left in place or deleted. A failed load skips archiving, so the file stays available for reprocessing. This keeps the file-level audit trail intact without requiring the tables themselves to accumulate history.
