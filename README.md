# Retail Sales Lakehouse - Incremental ETL Pipeline
An end-to-end Data Engineering portfolio project built on Microsoft Fabric, implementing a medallion architecture (bronze → silver → gold) with PySpark notebooks, Delta Lake, and Power BI (Direct Lake).

Built as part of an MSBI Developer → Data Engineer transition, alongside DP-700 (Microsoft Fabric Data Engineer Associate) certification prep.
---
# 🎯 Project Goals
🔸Design and build a production-style ETL pipeline: truncate-and-load staging, incremental dimensional merge  
🔸Implement a proper star schema with SCD Type 2 (dim_customer, fact_order_status_history)  
🔸Practice real incremental patterns: watermark-based extraction, MERGE/upsert logic  
🔸Orchestrate the pipeline with separate Fabric notebooks per layer, chained by a Fabric Data Pipeline  
🔸Serve the final gold layer through Power BI using Direct Lake mode  
🔸Apply CI/CD via Fabric Git integration and deployment pipelines (dev → test → prod)
---
# 🏗️ Architecture
                                         Source file(s) arrive
                                  (email / SharePoint / folder drop)
                                                  │
                                                  ▼
                   ┌──────────────────────────────────────────────────────────────────┐
                   │                        Microsoft Fabric                          │
                   │                                                                  │
                   │  ┌───────────────┐   ┌───────────────┐   ┌─────────────────────┐ │     ┌───────────┐
                   │  │  RAW / BRONZE │──▶│    SILVER     │──▶│        GOLD        │ ┼───▶│ Power BI  │
                   │  │ truncate&load │   │ truncate&load │   │  incremental MERGE  │ │     │Direct Lake│
                   │  │ (today's file │   │ (today's      │   │  (Type 1/2 — the    │ │     └───────────┘
                   │  │  only, no     │   │  cleaned      │   │  only layer that    │ │
                   │  │  history kept)│   │  batch only)  │   │  persists history)  │ │
                   │  └───────────────┘   └───────────────┘   └─────────────────────┘ │
                   │         ▲                                                        │
                   │         │  orchestrated by Fabric Data Pipeline (scheduled)      │
                   └──────────────────────────────────────────────────────────────────┘
🔸Raw/Bronze — Truncate & load. Holds only whatever file(s) arrived in this run; no persistent history of its own. Mirrors how real source systems typically behave — they hand you today's file, they don't track history for you.  
🔸Silver — Truncate & load. Cleaned/validated version of today's raw batch: correct data types, NOT NULL enforcement, dedup of exact duplicates. Still transient, still no accumulated history.  
🔸Gold — The only layer that persists history. Incremental MERGE: Type 1 for fact_orders (insert-once, immutable), Type 2 for dim_customer and fact_order_status_history (expire-old/insert-new).  
🔸Serving — Power BI semantic model in Direct Lake mode, reading gold Delta tables directly from OneLake (no import/refresh cycle).
---
# Pipeline flow (per scheduled run):  
1. Source system drops file(s) into a folder / SharePoint / mailbox — landed in Files/incoming/
2. Fabric Data Pipeline triggers on schedule, picks up file(s) one at a time
3. Raw: truncate table, load the incoming file as-is
4. Archive: once the raw load succeeds, move the source file from Files/incoming/ to Files/archive/YYYY-MM-DD/ (via mssparkutils.fs.mv()) — this is the only place the original file's history is preserved, since the raw/silver tables themselves don't retain it. A failed load should not archive the file, so it remains available for reprocessing.
5. Silver: truncate table, load a cleaned/validated version of what's in raw
6. Gold: MERGE the cleaned silver data into the persistent dim/fact tables (Type 1/2 as appropriate)
7. Power BI reports read gold via Direct Lake — always current, no separate refresh step
---
# 📋 Table Inventory
__Layer__ |	__Table__ |	__Load Pattern__ | __Purpose__
-- | -- | -- | --
🔸Raw/Bronze | Customer |	Truncate & load |	Whatever customer file arrived this run (master or change feed)  
🔸Raw/Bronze | Orders	| Truncate & load | Whatever order file arrived this run  
🔸— |	Files/archive/<date>/ |	Append (files, not a table) |	Preserves each processed source file, since raw/silver tables themselves retain no history  
🔸Silver | Customer | Truncate & load | Cleaned/validated version of today's raw batch  
🔸Silver | Orders | Truncate & load | Cleaned/validated version of today's raw batch  
🔸Gold | dim_customer | Incremental MERGE (Type 2) | Persisted customer dimension — full attribute history  
🔸Gold | fact_orders | Incremental MERGE (Type 1) | Persisted, immutable order line items — grain: one row per order line item  
🔸Gold | fact_order_status_history | Incremental MERGE (Type 2) | Persisted, full line-item-level status lifecycle  
🔸Gold | dim_date | Generated once | Calendar dimension, built via Spark's sequence() function  
🔸Gold | (reporting views) | — | Aggregates on the star schema — e.g. spend by category, monthly trends
---
# 🧩 Dimensional Model — Slowly Changing Dimension (Type 2)

🔸Customer attributes (City, Country, Email) are tracked as a proper SCD Type 2 dimension, rather than overwritten in place (Type 1). This preserves history — e.g. what a customer's city was at the time a given order was placed — instead of losing that context.

<ins> _dim_customer — SCD Type 2_ </ins>  
  __Column__ | __Purpose__  
  -- | --
🔸CustomerSK | Surrogate key — uniquely identifies each version of a customer row  
🔸CustomerID | Natural/business key — same across all versions of a customer  
🔸CustomerName, Email, City, Country | Tracked attributes  
🔸EffectiveStartDate | When this version became active  
🔸EffectiveEndDate | When this version stopped being active (9999-12-31 if current)  
🔸IsCurrentRecord | 1 = current version, 0 = historical

Initial load comes from customer_master.csv. Ongoing changes arrive via customer_profile_changes.csv — a full-row snapshot feed (same schema as master, no explicit "what changed" metadata) containing both updates to existing customers and brand-new customers. The pipeline itself determines INSERT vs. UPDATE by comparing incoming CustomerIDs against the current dim_customer.

<ins> _fact_orders — immutable, Type 1_ </ins>  
Grain: one row per order line item (an order can contain multiple products). Columns: OrderLineID (surrogate), OrderID, LineNumber, CustomerID (FK), OrderDate, Product, Category, Quantity, UnitPrice, PaymentMode. Created once, never versioned — core order details don't change after placement.

<ins> _fact_order_status_history — SCD Type 2, line-item grain_ </ins>
__Column__ | __Purpose__
-- | --
🔸OrderStatusHistorySK | Surrogate key  
🔸OrderID, LineNumber | Composite key — matches fact_orders' grain  
🔸CustomerID | Denormalized for query convenience  
🔸OrderStatus | Status of this line item at this point in time  
🔸StatusEffectiveStartDate / StatusEffectiveEndDate | Validity range (9999-12-31 if current)  
🔸IsCurrentRecord | 1 = current status, 0 = historical

Kept as a separate table from fact_orders, not merged in — see docs/learning-log.md for why (short version: merging would duplicate immutable measures like Quantity/UnitPrice on every status change, risking silent revenue double-counting in any query that forgets to filter IsCurrentRecord = 1).

Grain is OrderID + LineNumber, not OrderID alone — real e-commerce systems (Amazon, Flipkart) allow individual items in one order to be cancelled or shipped independently, so status is a line-item-level concept.

<ins> _dim_date — generated_ </ins>  
Standard calendar dimension (DateKey, FullDate, Year, Quarter, Month, MonthName, DayName, IsWeekend), generated via Spark SQL's sequence() function rather than a recursive CTE (Spark SQL doesn't support recursion).
---
# 📊 Dataset
All files share one canonical customer pool — CustomerID and attributes match exactly across every file.  
__File__ | __Rows__ | __Purpose__
-- | -- | --
🔸customer_master.csv | 5,000 | Day 0 baseline load for dim_customer  
🔸customer_profile_changes.csv | 85 | Full-row snapshot feed — 60 updates + 25 new customers  
🔸orders_historical.csv | ~195,000 (100,000 orders) | Day 0 bulk load — line-item grain  
🔸orders_incremental_day1/2/3.csv |	~900–1,000 each | New orders + status updates (cascading to all line items of an updated order)

_CustomerID_ — unique 6-digit number (not sequential), consistent across all files.  
<ins> _Customer Schemas (master and change feed share identical columns):_ </ins> CustomerID, CustomerName, Email, City, Country, CustomerSince  
<ins> _Orders Schema:_ </ins> OrderLineID, OrderID, LineNumber, CustomerID, OrderDate, LastModifiedTS, Product, Category, Quantity, UnitPrice, PaymentMode, OrderStatus — order-only fields; customer attributes are deliberately excluded and retrieved via join to dim_customer.  
A single consolidated script (scripts/generate_final_dataset.py) produces all 6 files with guaranteed cross-file consistency — included in this repo for reproducibility.
---
# 🛠️ Tech Stack  
  __Layer__ | __Tool__  
  -- | --
🔸Storage | OneLake (Fabric Lakehouse)  
🔸Compute | Fabric Spark Notebooks (PySpark, Spark SQL)  
🔸Table format | Delta Lake  
🔸Orchestration | Fabric Data Pipelines  
🔸Real-time (planned) | Eventstream / Eventhouse (KQL)  
🔸Reporting | Power BI — Direct Lake mode  
🔸CI/CD | Fabric Git Integration + Deployment Pipelines  
🔸Version control | Git / GitHub
---
# 📓 Notebooks / Pipeline Structure  
Each layer transition is a separate notebook, chained together by a Fabric Data Pipeline — mirroring real production separation of concerns:  
__Notebook__ | __Purpose__
-- | --
01_raw_customer | Truncate & load raw Customer from Files/incoming/, then archive the source file to Files/archive/<date>/
02_raw_orders | Truncate & load raw Orders from Files/incoming/, then archive the source file
03_silver_customer | Clean/validate raw Customer → silver Customer
04_silver_orders | Clean/validate raw Orders → silver Orders
05_gold_dim_customer | MERGE silver Customer into dim_customer (SCD2)
06_gold_fact_orders | MERGE silver Orders into fact_orders (Type 1)
07_gold_fact_order_status_history | Derive status changes, MERGE into status history (SCD2)
08_gold_dim_date | One-time generation of dim_date
09_gold_aggregates | Reporting views/aggregate tables
---
# 📁 Repository Structure
    fabric-lakehouse-de-transition/
    ├── README.md
    ├── scripts/
    │   └── generate_final_dataset.py   # single source of truth for all synthetic data
    ├── notebooks/                      # exported PySpark notebooks, one per layer transition
    ├── sql/                            # Spark SQL / T-SQL scripts (views, MERGE statements)
    ├── docs/
    │   ├── architecture-diagram.png
    │   └── learning-log.md
    └── .gitignore

# ✅ Progress Checklist
✔️ Fabric workspace + Lakehouse setup  
✔️ Final dataset generated and consistency-verified (customer master, change feed, historical + incremental orders)  
✔️ Raw/Bronze: initial Customer + Orders load  
✔️ Switch raw/bronze + silver to truncate & load pattern (separate notebooks per layer)  
✔️ File archiving: move processed source files from Files/incoming/ to Files/archive/<date>/ after successful raw load  
✔️ Silver: Customer, Orders — cleaning, NOT NULL enforcement, dedup  
✔️ Gold: dim_customer — SCD Type 2 MERGE  
✔️ Gold: fact_orders — Type 1 MERGE  
✔️ Gold: fact_order_status_history — SCD2 MERGE, line-item grain  
✔️ Gold: dim_date — generated  
✔️ Gold: reporting aggregate views  
✔️ Orchestration via Fabric Data Pipeline (scheduled, chaining all notebooks)  
✔️ Monitoring / data quality checks  
✔️ Power BI report on gold layer (Direct Lake)  
✔️ CI/CD: Fabric Git integration + deployment pipeline (dev/test/prod)  
✔️ DP-700 certification  
--- 
# 📓 Learning Log
Key design decisions and reasoning — including a couple of real mistakes caught and corrected along the way — are tracked in docs/learning-log.md.  
---
# 🔗 Background
This project is part of a career transition from MSBI Developer (SSIS, SQL Server, Power BI, some ADF) to Data Engineer, focused on building hands-on depth in Spark/PySpark, lakehouse architecture, and modern orchestration — while retaining strengths in SQL and BI reporting. The truncate-and-load-staging → incremental-merge-to-dimensional-model pattern used here maps directly onto classic SSIS staging-table patterns, just implemented with Fabric notebooks and Delta MERGE instead.
