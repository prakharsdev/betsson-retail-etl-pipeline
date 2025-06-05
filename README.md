# Retail Sales ETL Pipeline and Star Schema Modeling for Betsson Group

## Project Overview

I built this end-to-end ETL pipeline from scratch to address the data engineering assignment provided by **Betsson Group**. The objective was to design a clean, production-grade pipeline that transforms messy invoice data into a reliable, analysis-ready star schema using **Kimball dimensional modeling**. My approach emphasizes modular design, testability, transparency in assumptions, and business-ready outputs.

**The pipeline simulates a real-world retail analytics use case and includes:**

* **Automated ETL workflow** for extracting, cleaning, transforming, and loading raw sales data
* **Detection, logging, and explanation of data abnormalities and assumptions** (`assumptions.txt`, `pipeline.log`)
* **Star schema modeling** using Kimball methodology with clear `fact_sales` and `dim_*` tables
* **Business-ready aggregation reports** for revenue by country, customer, and monthly trends
* **Exploratory and validation notebooks** for profiling and reporting (`notebooks/`)
* **Robust unit testing** with full-dataset validation, relationship checks, and logic assertions (`tests/`)
* **Modular source code** structure with centralized config and reusable components
* **Deliverables in clean CSV format** for easy sharing, analysis, and validation
* **Change-ready structure** designed for easy migration to databases, Airflow, or Spark pipelines

---

## Folder Structure

```
Betsson/
├── data/
│   ├── raw/
│   │   └── transactions.csv             # Raw invoice data (originally Invoices_Year_2009-2010.xls) from Betsson — contains sales transactions
│   ├── processed/
│   │   └── final_output.csv             # Cleaned data after removing invalid/missing records, used to build star schema
│   └── warehouse/                       # Final star schema output using Kimball methodology
│       ├── dim_customer.csv             # Customer dimension table: includes customer_key, Customer ID, Country
│       ├── dim_product.csv              # Product dimension table: includes product_id, StockCode, Description
│       ├── dim_date.csv                 # Date dimension table: includes date_key, InvoiceDate, year, month, day, weekday
│       └── fact_sales.csv               # Fact table with foreign keys (product_id, customer_key, date_key) and measures (Quantity, Price, TotalPrice)
├── output/
│   ├── agg_by_country.csv               # Aggregation report: total revenue per country
│   ├── agg_by_customer.csv              # Aggregation report: total revenue per customer
│   ├── agg_monthly.csv                  # Aggregation report: total monthly revenue (InvoiceDate resampled by month)
│   ├── assumptions.txt                  # Auto-generated list of data abnormalities and assumptions (pre- and post-cleaning)
│   ├── pipeline.log                     # Logs for ETL pipeline execution (info, warnings, timing, failures)
│   └── mismatched_totalprice_rows.csv   # (Debug aid) rows where TotalPrice != Quantity * Price — used to flag quality issues
│
├── notebooks/                           # Jupyter Notebooks for EDA and profiling
│   ├── profiling_analysis.ipynb         # Raw data profiling: nulls, duplicates, price/qty issues
│   └── reporting_analysis.ipynb         # Revenue insights: top countries, customers, products, monthly trends
│
├── src/                                 # Source code for the ETL pipeline
│   ├── extract.py                       # Logic for reading raw CSVs and validating schema
│   ├── transform.py                     # All data cleaning, validation, and transformation logic
│   ├── load.py                          # Logic to save outputs to proper directories
│   ├── utils.py                         # Reusable helper functions
│   ├── config.py                        # Centralized file paths and config variables
│   └── dw/
│       └── dw_model.py                  # Builds fact/dimension tables based on the star schema
│
├── tests/                               # Unit tests to validate transformation and modeling logic
│   ├── test_transform.py                # Tests for cleaning logic, edge case handling
│   └── test_dw_model.py                 # Tests for schema structure and key constraints
│
├── main.py                              # Entry point to run the ETL pipeline end-to-end
└── README.md                            # Project documentation and design decisions

```

---

## How to Run the Pipeline

### 1. Install dependencies (via pip or conda)

This project assumes Python 3.11.9 and libraries like `pandas`, `pytest`, etc.

```bash
pip install -r requirements.txt  # if using requirements file
```

### 2. Run the ETL pipeline

```bash
python main.py
```

This will:

* Load the raw file (`transactions.csv`)
* Clean and validate data
* Log abnormalities (pre/post cleaning)
* Generate cleaned output, dimensions, and fact tables
* Create business aggregations
* Save everything to the `/data/` and `/output/` directories

### 3. Run tests

```bash
pytest tests/test_transform.py
```
```bash
pytest tests/test_dw_model.py
```

Ensures correctness of transformation logic and schema integrity.

---

## Design Decisions (with Rationale)

### Dimensional Modeling (Kimball-style)

I chose a **star schema** because it aligns with analytical workload requirements:

* It simplifies joins for business analysts and BI tools.
* Dimensions like customer, product, and date allow filtering and slicing across natural business entities.
* Surrogate keys help isolate historical and slowly changing dimensions.

This design supports scalability, clarity, and performance in downstream tools like Looker, Tableau, or Power BI.

### Data Cleaning Rules

These decisions were guided by both **data integrity needs** and **business logic**:

* Removing nulls in `Customer ID`, `Price`, and `Description` prevents corrupted metrics and broken joins.
* Filtering `Quantity <= 0` and `Price <= 0` ensures revenue is computed only from valid sales.
* Suspicious descriptions (`?`, `UNKNOWN`) are often placeholders that do not map to valid SKUs.
* Deduplication ensures we don't double count revenue.
* `TotalPrice` is computed explicitly to prevent reliance on potentially incorrect source fields.

## Abnormality Detection (Why I Log Instead of Drop?)

Instead of silently cleaning everything, I built logging into the pipeline to capture anything odd that I cleaned **or** decided to keep.

Here’s why that matters to me:

* **Transparency for future you (or me)**:
  I have seen pipelines where someone cleans something, forgets about it, and months later no one knows why certain rows are missing. This log ensures the next person (or my future self) knows exactly what was flagged and why.

* **Audit readiness**:
  If this were in production, say, for financial or regulatory reporting, we’d need to explain why revenue dropped last month. Being able to trace that back to dropped rows with `NULL Price` or weird `POST` codes makes that conversation much easier.

* **Business involvement**:
  I don’t want to assume what's right in every case. Maybe marketing wants to keep system rows for spend tracking. Maybe outliers are valid enterprise deals. So instead of removing everything that "looks weird", I just log it and let the business decide.

For example, if there are 700 rows with `StockCode = POST` and `TotalPrice = 54.00`, someone from logistics might say: "Ah, that’s our shipping fee, we actually *do* want to report it." I’d rather give them the option than erase it.

The log file — `assumptions.txt` — is automatically created every time the pipeline runs. It compares what was found **before cleaning** and what remained **after cleaning**, so you can see exactly what was removed vs. what was just flagged.


---

### Aggregations

These key business aggregations are automatically generated by the pipeline and saved as CSVs, making them instantly ready for use in reporting tools, dashboards, or stakeholder presentations.

| Aggregation Metric         | Output File           | Business Purpose                                  |
| -------------------------- | --------------------- | ------------------------------------------------- |
| **Revenue by Country**     | `agg_by_country.csv`  | Understand market performance by geography        |
| **Revenue by Customer**    | `agg_by_customer.csv` | Identify high-value customers and segments        |
| **Monthly Revenue Trends** | `agg_monthly.csv`     | Analyze seasonality, trends, and forecast revenue |

---

All aggregations are saved to CSV so they can be dropped into BI tools or spreadsheets without additional effort.

### Notebooks for Exploration & Validation

In addition to building the ETL pipeline and dimensional model, I created two Jupyter notebooks to support data validation and exploratory analysis:

#### 1. `profiling_analysis.ipynb`

This notebook runs checks from `profile_raw_data.py` to explore the raw dataset. It includes:

* Column-level summaries, null checks, duplicates
* Outlier identification in `Quantity` and `Price`
* Validation of `InvoiceDate` ranges and suspicious descriptions
* Summary counts for invoices, customers, countries, and metadata codes

This step helped me shape reliable cleaning rules and identify hidden patterns that could impact downstream metrics.

#### 2. `reporting_analysis.ipynb`

This notebook loads the final star schema tables and performs key business analyses:

* Top 5 countries by total revenue
* Monthly revenue trends (visualized with a line chart)
* Top 10 customers by total spend
* Top 10 products by quantity sold
* Outlier detection based on extreme quantities or prices

I built these with business users in mind. Anyone using Excel or BI tools could replicate the logic or extract insights from the CSVs we generate.

---

### File Renaming for Clarity

The original raw dataset was named `Invoices_Year_2009-2010.csv`. For clarity and maintainability, I renamed it to `transactions.csv`.

Here’s why:

* It better reflects the content and structure. The file contains all transactions, not just invoice headers.
* Naming it generically allows future pipeline runs with different date ranges or file versions, without requiring code changes.
* It’s a small change, but one that mirrors how we’d treat raw data sources in production.

The renaming is noted in the README and handled via `config.py`, so it’s easy to switch back if needed.

### Profiling Before Design

Before modeling, I used `profile_raw_data.py` to:

* Understand field distributions, duplicates, and nulls
* Identify system-generated records (e.g. StockCodes)
* Validate uniqueness and relationships across Invoice, StockCode, and Customer

This step ensured my downstream logic was both accurate and relevant for business usage.

### Testing Strategy

By using `pytest`, I enforce confidence in every transformation:

* I validate foreign key consistency across the fact and dimension tables.
* I confirm that data cleaning removes bad rows without affecting valid entries.
* I check that my assumptions (e.g., system StockCodes having 0 revenue) hold true post-cleaning.

This makes the pipeline safe to use in automation or CI/CD contexts.

NB! Because the dataset is small (~500k rows), I ran full checks in my unit tests that includes cleaning, revenue logic, and FK relationships. But in a real production setup with millions of rows, I’d sample key slices, validate aggregated outputs, or use mocks and fixture stubs to keep tests fast and reliable.

### Logging

All major events and timings are logged in `pipeline.log`, allowing future developers or data ops teams to:

* Troubleshoot failures
* Review pipeline durations
* Identify where transformations occurred

If the pipeline fails, full tracebacks are saved, ensuring maintainability and rapid debugging.

---

## Assumptions & Data Issues

I wanted this pipeline to feel like something you'd actually rely on in production not just a data dump that *looks* clean but hides messy realities. So I made a few practical assumptions and added logic to spot data issues early and document them in `output/assumptions.txt`.

Here’s what I assumed and how I handled the quirks in the data:

* **Missing critical fields**:
  If a row was missing a `Customer ID`, `Price`, or `Description`, I dropped it. For example, if a row had a `Quantity` of 5 but no price, we can’t trust the revenue calculation. And if there’s no customer ID, it can’t link to the dimension model, so it’s gone.

* **Negative or zero values**:
  I removed any rows where `Quantity` or `Price` was less than or equal to 0. These don’t make sense in a sales context. A row with `Quantity = -5` and `Price = 2.50` might be a refund or data entry error, but since I didn’t have refund logic in scope, I chose to exclude them.

* **System-generated StockCodes (`POST`, `ADJUST`, `CHECK`)**:
  These are often used for shipping, manual adjustments, or internal accounting. I didn’t drop them, I kept them in the dataset so someone from finance or ops can review them, but I made sure to flag them clearly. I didn’t override their revenue (`TotalPrice`) to 0 because that’s a business call, not an engineering one.

* **Outliers**:
  I found a few rows with crazy values like `Quantity = 1200` or `Price = 1,500.00`. I didn’t drop them, maybe someone really did order 1200 paperweights for an office giveaway. But I logged them in the assumptions file so they don’t go unnoticed.

* **Duplicate rows**:
  I removed exact duplicates, same `Invoice`, `StockCode`, `Quantity`, everything, to avoid double-counting. But I kept duplicates on `(Invoice, StockCode)` because sometimes customers do buy the same item twice in one order, and it was safer to retain those than risk undercounting.

* **Customers in multiple countries**:
  A few `Customer ID`s showed up with more than one country. Instead of treating that as an error, I assumed it could be a valid B2B case (like a corporate account ordering from different offices). I kept those rows and flagged the pattern in the logs.

NB! I tried to clean what was clearly broken, and flag what looked suspicious without being too aggressive. The goal was to preserve context and give analysts and stakeholders room to make business calls on edge cases.

---

## ERD Diagram (Star Schema)

```
dim_product       dim_customer        dim_date
   |                  |                   |
   |                  |                   |
   +--------+---------+--------+----------+
                            |
                       fact_sales
```

**fact\_sales.csv**

* Foreign Keys: `product_id`, `customer_key`, `date_key`
* Measures: `Quantity`, `Price`, `TotalPrice`

## Relational Integrity in CSVs

**Note**: While the star schema defines clear **primary and foreign key relationships** (e.g., `customer_key` in `fact_sales.csv` references `dim_customer.csv`), CSV files are **flat files** and do **not enforce relational constraints**. These relationships are modeled conceptually and must be validated via logic and testing (e.g., using `pytest`) not automatically enforced like in SQL-based databases.

---

## Final Deliverables

| Type    | Files                                                          |
| ------- | -------------------------------------------------------------- |
| Code    | `main.py`, `src/`, `tests/`, `notebooks/`                      |
| Data    | `transactions.csv`, `processed/final_output.csv`, `warehouse/` |
| Reports | `agg_by_*.csv`, `assumptions.txt`, `pipeline.log`              |
| Docs    | `README.md`, ERD (optional image or markdown)                  |

---

###  Why I Choose CSVs (as Deliverables)

The assignment allowed flexibility in tooling and formats, and since the goal was to showcase a clean, modular data warehouse model using Kimball methodology, I chose **CSVs** for the following reasons:

* **Universally accessible**: CSVs can be opened in Excel, Python, or any BI tool without additional setup, making it easy for both technical and non-technical stakeholders to validate the output.

* **Simplicity**: For a test project without a production-grade database backend, CSVs offer a no-friction way to deliver dimensional tables and aggregations without dealing with database overhead.

* **Transparency**: CSVs make it easy to inspect outputs directly and align with the spirit of the assignment to focus on assumptions, cleaning logic, and modeling best practices.

If this were a real-world pipeline, I’d deliver these outputs to a data warehouse like Snowflake or BigQuery, but for the purpose of this self-contained assignment, CSVs were the most practical, portable, and review-friendly choice.

## Notes

* All file paths are centralized in `config.py`
* Pipeline is fully local, no database or cloud setup required
* Easily extendable to SQL-based DBs (e.g. SQLite or MSSQL)
* Ready for CI/CD with test hooks

---

## Future Scope & Scalability

Designing for scale and adaptability is at the core of any modern data platform. Below, I have outlined the paths I would pursue to evolve this pipeline from a solid MVP into a scalable, enterprise-ready analytics system with both business impact and engineering feasibility in mind.

---

### Vertical Scalability (Data Volume)

**Current State**:
The ETL pipeline is built with pandas and runs entirely in-memory. This is suitable for datasets up to \~5 million rows on a standard laptop.

**Next Steps**:

* **Migration to PySpark**: As data grows (10M+ rows), I’d refactor transformations to PySpark to enable parallelism and distributed compute.
* **Chunked CSV reads/writes**: Introduce batching mechanisms for memory efficiency if we stay on pandas for longer.

**Business Impact**:

* Enables the ingestion and transformation of full historical data (not just samples).
* Supports advanced analysis like LTV cohorts and seasonality at scale.

---

### Horizontal Scalability (Team & Infrastructure)

**Current State**:
Pipeline is orchestrated with a single Python script. Simple but not scalable for team collaboration or production SLAs.

**Next Steps**:

* **Airflow DAGs**: Modularize extraction, transformation, loading, and logging into separate DAG tasks.
* **Dockerization**: Package each step for reproducibility and platform independence.
* **Cloud Migration**:

  * Storage: Migrate outputs to S3, GCS, or Azure Blob.
  * Processing: Use Databricks or EMR for scale-out Spark jobs.
  * Warehouse: Load dimensional tables into BigQuery/Snowflake/Redshift.

**Business Impact**:

* Enables 24/7 reliability and monitoring (especially crucial for gaming/retail ops).
* Simplifies cross-functional ownership — BI teams can plug in Looker/Tableau directly to cloud tables.
* Reduces time-to-insight through autoscaling.

---

### Incremental & Real-Time Processing

**Current State**:
The pipeline is batch-based and processes the full dataset on every run.

**Next Steps**:

* **Incremental Loads**: Filter by `InvoiceDate` to process only new records (e.g., last 24 hours).
* **Streaming**: Use Kafka/Flink/Spark Streaming to support near real-time updates to star schema tables.

**Tech Options**:

* **CDC from upstream OLTP DBs** using Debezium or Fivetran.
* **Delta Lake or Iceberg** to support ACID and streaming-compatible warehouse writes.

**Business Impact**:

* Real-time dashboards for campaign performance or fraud detection.
* Reactive marketing (e.g., instant notifications for abandoned carts or high-value purchases).

---

### Advanced Data Modeling

**Current State**:
Uses Type-1 slowly changing dimensions via surrogate keys.

**Next Steps**:

* Implement **SCD Type-2** for tracking customer attribute changes over time (e.g., country migration).
* Add **dim\_store**, `dim_sales_channel`, or `dim_campaign` if marketing or channel data is integrated.
* Create **aggregated fact tables** (e.g., weekly revenue snapshots for CFO-level views).

**Business Impact**:

* Historical accuracy of reports preserved (e.g., when customers move countries or products are rebranded).
* Enables scenario modeling (e.g., “What if we improve retention in Germany by 10%?”).

---

### Metadata, Governance, and Testing

**Current State**:
Basic logging and unit tests validate assumptions and data health.

**Next Steps**:

* **Great Expectations** for data contracts, column-level tests, and automated documentation.
* **Data Lineage**: Use tools like OpenLineage or dbt docs for impact analysis and traceability.
* **Audit Trails**: Record “who changed what and when” for data compliance.

**Business Impact**:

* Builds trust with finance/legal teams.
* Avoids costly pipeline failures by surfacing silent errors (e.g., format drift, schema mismatch).

---

### Future Features & Value Additions

| Feature                     | Impact                                                             |
| --------------------------- | ------------------------------------------------------------------ |
|  Parquet/Delta formats      | Faster I/O, reduced storage costs                                  |
|  KPI Layer on Top of Fact   | Business logic layer for Net Revenue, AOV, Refund Rate             |
|  Anomaly Detection          | Auto-flag sales drops or outliers using ML                         |
|  Access Controls            | Role-based access to sensitive data (PII masking, GDPR compliance) |
|  Embedded Dashboards        | Push analytics directly into operational tools or CRM              |

---

## Final Thoughts

A well-architected data pipeline isn’t just about rows and columns, it’s about unlocking insights, enabling agility, and supporting the business in making decisions with confidence.
This project is designed to grow gracefully across:

* **Volume** (from 500K to billions of rows)
* **Velocity** (from daily to near real-time)
* **Variety** (from invoices to campaigns, customers, and channel data)

I’m confident that this system can evolve into a fully-fledged, production-grade platform that empowers Betsson to extract meaningful value from their data at scale.



