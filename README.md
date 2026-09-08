# Retail Sales Lakehouse - Incremental ETL Pipeline
An end-to-end Data Engineering portfolio project built on Microsoft Fabric, implementing a medallion architecture (bronze → silver → gold) with PySpark notebooks, Delta Lake, and Power BI (Direct Lake).

Built as part of an MSBI Developer → Data Engineer transition, alongside DP-700 (Microsoft Fabric Data Engineer Associate) certification prep.

# 🎯 Project Goals
🔸Design and build a production-style ETL pipeline: historical bulk load + daily incremental ingestion
🔸Implement medallion architecture (bronze/silver/gold) using Fabric Lakehouse and Delta Lake
🔸Practice real incremental patterns: watermark-based extraction, MERGE/upsert logic for updated records
🔸Orchestrate the pipeline with Fabric Data Pipelines, add monitoring and basic data quality checks
🔸Serve the final gold layer through Power BI using Direct Lake mode
🔸Apply CI/CD via Fabric Git integration and deployment pipelines (dev → test → prod)

# 🏗️ Architecture
                    ┌─────────────────────────────────────────────┐
                    │              Microsoft Fabric               │
                    │                                             │
  Raw CSV Files     │   ┌──────────┐    ┌──────────┐   ┌─────────┐│    ┌───────────┐
  (historical +     │──▶  BRONZE  │───▶│  SILVER  │──▶│  GOLD   |───▶│ Power BI  │
  daily incremental)│   │ (raw)    │    │ (cleaned)│   │ (agg.)  ││    │Direct Lake│
                    │   └──────────┘    └──────────┘   └─────────┘│    └───────────┘
                    │        ▲                                    │
                    │        │orchestrated by Fabric Data Pipeline│
                    └─────────────────────────────────────────────┘
🔸Bronze: Raw ingested data, minimal transformation, schema-on-read
🔸Silver: Cleaned, deduplicated, conformed data — MERGE/upsert applied here for incremental updates
🔸Gold: Business-level aggregates, ready for reporting
🔸Serving: Power BI semantic model in Direct Lake mode, reading gold Delta tables directly from OneLake (no import/refresh cycle)

# 🧩 Dimensional Model — Slowly Changing Dimension (Type 2)

🔸Customer attributes (City, Country, Email) are tracked as a proper SCD Type 2 dimension, rather than overwritten in place (Type 1). This preserves history — e.g. what a customer's city was at the time a given order was placed — instead of losing that context.

[dim_customer (silver/gold layer):]
  [Column]	                            [Purpose]
🔸CustomerSK	                          Surrogate key — uniquely identifies each version of a customer row
🔸CustomerID	                          Natural/business key — same across all versions of a customer
🔸CustomerName, Email, City, Country	  Tracked attributes
🔸EffectiveStartDate	                  When this version became active
🔸EffectiveEndDate	                    When this version stopped being active (NULL / far-future date if current)
🔸IsActive	                            1 for the current version, 0 for historical versions

When a change arrives via customer_profile_changes.csv:
1. The current active row for that CustomerID is expired (IsActive = 0, EffectiveEndDate set)
2. A new row is inserted with the updated attribute(s), IsActive = 1, EffectiveEndDate = 9999-12-31

The initial load of dim_customer comes from customer_master.csv (Day 0 baseline, one row per customer, all marked IsActive = 1). Ongoing changes then arrive exclusively through the customer_profile_changes.csv feed — kept as a separate source from fact_orders, since customer master-data updates and order transactions are conceptually different feeds (and typically come from different systems in the real world, e.g. CRM vs. OMS).

fact_orders remains a standard fact table, joined to dim_customer on CustomerID (or the relevant CustomerSK at time of order — see docs/learning-log.md for the reasoning behind this design).

📜 Order Status History (SCD Type 2 pattern applied to order status)
Order status (Ordered → Processing → Shipped → Delivered, or Cancelled/Returned) is tracked as full history, not overwritten in place — so the complete lifecycle of every order is preserved, not just its current state.

[fact_order_status_history (silver layer):]
  [Column]	                  [Purpose]
🔸OrderID	                    Links back to fact_orders
🔸OrderStatus	                The status at this point in the order's lifecycle
🔸StatusEffectiveStartDate	  When this status became active
🔸StatusEffectiveEndDate	    When this status stopped being active (NULL if current)
🔸IsCurrentStatus	            1 for the order's current status, 0 for past statuses

◽Design note: fact_orders itself stays immutable — core order details (customer, product, quantity, amount, order date) don't change after placement. Only status changes over time, so status is split out into its own history table rather than applying full-row SCD2 to fact_orders. Each incremental order batch (which already carries OrderStatus + LastModifiedTS) is treated as a status transition event at the silver layer: the previous current-status row is expired, and a new one is inserted — the same expire-old/insert-new mechanic used for dim_customer, applied here to a fact attribute instead of a dimension attribute.

# 📊 Dataset
Synthetic retail dataset, designed specifically to demonstrate incremental ETL and SCD Type 2 patterns. All files share a single canonical customer pool, so CustomerID and customer attributes match exactly across every file — no synthetic mismatches.

  [File]	                        [Rows]	  [Purpose]
🔸customer_master.csv	            5,000	    Day 0 baseline — initial load source for the dim_customer dimension
🔸customer_profile_changes.csv	  60        Full-row customer snapshot feed — 60 updates + 25 new customers 
                                            — drives SCD Type 2 MERGE logic on dim_customer
🔸orders_historical.csv	          200,000	  Initial bulk load into fact_orders (Day 0)
🔸orders_incremental_day1.csv	    ~600	    New orders + ~100 status updates to existing orders
🔸orders_incremental_day2.csv	    ~635	    New orders + ~100 status updates
🔸orders_incremental_day3.csv	    ~536	    New orders + ~100 status updates

[CustomerID] — a unique 6-digit number, consistent across all files (not a simple 1, 2, 3… sequence, to better resemble a real-world customer identifier).

[Customer master schema:] CustomerID, CustomerName, Email, City, Country, CustomerSince

[Customer change feed schema:] identical to master — CustomerID, CustomerName, Email, City, Country, CustomerSince

▫️Modeled as a full-row snapshot feed, not a sparse diff — the source simply sends each customer's complete current record,   whether they're brand new or have an updated attribute. No ChangeType/ChangeDate metadata is provided (this mirrors how      many real-world source extracts behave — the source doesn't tell you what changed, your pipeline figures that out)
▫️Contains a mix of updates to existing customers (CustomerID already in master, one or more attributes changed) and brand-   new customers (CustomerID not in master at all) — so downstream logic must distinguish INSERT vs. UPDATE itself, typically   via a LEFT JOIN/MERGE against the current dim_customer on CustomerID
▫️Since no change timestamp is provided by the source, the pipeline's own load/batch date is used as EffectiveStartDate       when applying SCD2 — a common real-world compromise when source systems don't expose their own change timestamps

[Orders schema:] OrderID, CustomerID, CustomerName, Email, City, Country, OrderDate, LastModifiedTS, Product, Category, Quantity, UnitPrice, PaymentMode, OrderStatus
▫️CustomerName, Email, City, Country on each order are pulled directly from the same customer record as master (not           independently generated), so they always match exactly at load time
▫️OrderDate — used as the load/partition reference for the historical batch
▫️LastModifiedTS — watermark column driving incremental extraction and MERGE logic on fact_orders

# 🛠️ Tech Stack
  [Layer]	              [Tool]
🔸Storage	              OneLake (Fabric Lakehouse)
🔸Compute	              Fabric Spark Notebooks (PySpark, Spark SQL)
🔸Table format	        Delta Lake
🔸Orchestration	        Fabric Data Pipelines
🔸Real-time (planned)	  Eventstream / Eventhouse (KQL)
🔸Reporting	            Power BI — Direct Lake mode
🔸CI/CD	                Fabric Git Integration + Deployment Pipelines
🔸Version control	      Git / GitHub

# 📁 Repository Structure
fabric-lakehouse-de-transition/
├── README.md
├── notebooks/          # Exported PySpark notebooks (bronze, silver, gold layers)
├── sql/                # Spark SQL / T-SQL scripts (views, merge statements)
├── docs/
│   ├── architecture-diagram.png
│   └── learning-log.md   # Design decisions & "why X over Y" notes
└── .gitignore

# ✅ Progress Checklist
✔️ Fabric workspace + Lakehouse setup
✔️ Historical dataset generated (200K rows) + incremental batches designed
✔️ Customer master + profile change feed generated (for SCD Type 2), consistency-checked against orders data
✔️ Bronze layer: customer master + historical orders bulk load
✔️ Bronze layer: incremental ingestion (orders Day 1–3, customer profile changes)
✔️ Silver layer: dim_customer — SCD Type 2 implementation
✔️ Silver layer: fact_orders — cleaning, dedup, core immutable order details
✔️ Silver layer: fact_order_status_history — status history via SCD2-style pattern
✔️ Gold layer: business aggregates
✔️ Orchestration via Fabric Data Pipeline (scheduled + triggered)
✔️ Monitoring / data quality checks
✔️ Power BI report on gold layer (Direct Lake)
✔️ CI/CD: Fabric Git integration + deployment pipeline (dev/test/prod)
✔️ DP-700 certification
 
# 📓 Learning Log
Key design decisions and reasoning are tracked in docs/learning-log.md — written while building, not reconstructed after the fact.

# 🔗 Background
This project is part of a career transition from MSBI Developer (SSIS, SQL Server, Power BI, some ADF) to Data Engineer, focused on building hands-on depth in Spark/PySpark, lakehouse architecture, and modern orchestration — while retaining strengths in SQL and BI reporting.
