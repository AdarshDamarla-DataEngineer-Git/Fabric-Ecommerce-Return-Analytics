# Pipeline Architecture and Project Structure

## Contents

1. [Architecture overview](#architecture-overview)
2. [Pipeline flow](#pipeline-flow)
3. [Activity reference](#activity-reference)
4. [Watermark logic](#watermark-logic)
5. [Data layers](#data-layers)
6. [Warehouse design](#warehouse-design)
7. [Error handling and logging](#error-handling-and-logging)
8. [Project structure](#project-structure)
9. [Setup notes](#setup-notes)

## Architecture overview

```mermaid
flowchart LR
    GH[(GitHub<br/>monthly CSV)] -->|Copy activity| B[Bronze<br/>Lakehouse Files<br/>Parquet]
    B -->|Bronze_Silver_Notebook_Optimised| S[Silver<br/>Lakehouse Files<br/>Delta]
    B -.bad rows.-> Q1[(Quarantine<br/>bronze_silver)]
    S -->|Silver_Gold_Notebook_Optimised| ST[Warehouse staging<br/>Returns_Staging *_stg]
    S -.bad rows.-> Q2[(Quarantine<br/>silver_gold)]
    ST -->|Merge_Returns_Gold_Final| G[Warehouse Gold<br/>Returns_Gold star schema]
    G -->|semantic model refresh| SM[Direct Lake<br/>semantic model]
    SM --> BI[Power BI report]
    C[(Returns_Control<br/>Watermark + Metadata)] <--> P{{Fabric Pipeline}}
    P --> M[Email via Web activity]
```

| Layer | Location | Format | Purpose |
|---|---|---|---|
| Source | `Data/*.csv` in GitHub | CSV | One file per month |
| Bronze | Lakehouse `Files/bronze/ecommerce_returns/year=YYYY/month=MM/` | Parquet (snappy) | Raw copy of the month |
| Silver | Lakehouse `Files/silver/ecommerce_returns/` | Delta, partitioned by `Order_Year`, `Order_Month` | Cleaned, typed, validated order lines |
| Quarantine | Lakehouse `Files/quarantine/ecommerce_returns/{bronze_silver,silver_gold}/year=YYYY/month=MM/` | Delta | Rejected rows with the reasons |
| Staging | Warehouse schema `Returns_Staging` | Tables `*_stg` | Overwritten each run with the month's facts and dimensions |
| Gold | Warehouse schema `Returns_Gold` | Tables | Star schema, upserted. `Dim_Date` is a fixed calendar created once by `10_Create_Dim_Date.sql` |
| Control | Warehouse schema `Returns_Control` | Tables | Watermark and run metadata |
| Semantic model | `Ecommerce_Returns_Semantic_Model` | Direct Lake | Reads the Gold tables; refreshed by the pipeline after each load |
| Notebook audit | Lakehouse tables `audit_bronze_silver`, `audit_silver_gold` | Delta | One row per notebook run, one table per notebook (counts, status, duration) |

## Pipeline flow

The pipeline is `Ecommerce_Returns_Pipeline`. It runs with concurrency 1, so two runs never overlap.

```mermaid
flowchart TD
    A[Lookup_Watermark] --> B1[Set_watermark_year]
    A --> B2[Set_watermark_month]
    B1 --> C[Set_watermark_month_name]
    B2 --> C
    C --> D[Copy_Github_Bronze]
    D --> E[Set_bronze_record_processed]
    E --> F[Bronze_Silver_Notebook]
    F --> G[Set_silver_record_processed]
    G --> H[Silver_Gold_Stg_Notebook]
    H --> I[Set_gold_fact_Stg_record_processed]
    I --> J[Stored_Procedure_Stg_FinalGold]
    J --> K[Stored_Procedure_Update_Watermark]
    K --> R[Ecommerce_Returns_Sematic_Model]

    A -. failed .-> X[Set_Error_* activities]
    C -. failed .-> X
    D -. failed .-> X
    F -. failed .-> X
    H -. failed .-> X
    J -. failed .-> X
    K -. failed .-> X
    R -. failed .-> X

    X --> L[Stored_Procedure_Insert_Metadata]
    R --> L
    L -. failed .-> XL[Set_Error_Stored_Procedure_Insert_Metadata]
    L --> M{If_Check_Error}
    XL --> M
    M -- error_message set --> N[Send_Failure_Email] --> O[Fail_Pipeline]
    M -- no error --> P[Send_Success_Email]
```

The "Set_Error_*" activities and the metadata logging always run, so every run, successful or not, ends up in `Returns_Control.Pipeline_Metadata`. The semantic model refresh is the last data step: it starts only after the watermark has been updated.

## Activity reference

| Activity | Type | What it does |
|---|---|---|
| `Lookup_Watermark` | Lookup (Warehouse) | Reads the latest `Last_Processed_Year` and `Last_Processed_Month` for this pipeline |
| `Set_watermark_year` / `Set_watermark_month` | Set variable | Compute the month to load: reprocess parameters if both are set, otherwise the month after the watermark (December rolls to January of the next year) |
| `Set_watermark_month_name` | Set variable | Builds the month name (for example `March`) used in the file name. Fails on an invalid reprocess input |
| `Copy_Github_Bronze` | Copy | Downloads `Ecommerce_Returns_Refunds_<YYYY>_<MM>_<MonthName>.csv` over HTTP and writes Parquet to the Bronze path |
| `Set_bronze_record_processed` | Set variable | Stores `rowsCopied` |
| `Bronze_Silver_Notebook` | Notebook | Runs `Bronze_Silver_Notebook_Optimised` with `p_year`, `p_month` and `p_run_id` (the pipeline run ID, written to the notebook audit table). Returns the number of valid rows merged |
| `Set_silver_record_processed` | Set variable | Stores the notebook exit value |
| `Silver_Gold_Stg_Notebook` | Notebook | Runs `Silver_Gold_Notebook_Optimised` with the same three parameters. Returns the number of fact rows staged |
| `Set_gold_fact_Stg_record_processed` | Set variable | Stores the notebook exit value |
| `Stored_Procedure_Stg_FinalGold` | Stored procedure | `Returns_Staging.Merge_Returns_Gold_Final` merges staging into Gold in one transaction |
| `Stored_Procedure_Update_Watermark` | Stored procedure | `Returns_Control.Update_Watermark` advances the watermark |
| `Ecommerce_Returns_Sematic_Model` | Semantic model refresh | Refreshes `Ecommerce_Returns_Semantic_Model` after the watermark is updated and waits for it to finish (transactional commit, whole model). The activity name is spelled this way in the pipeline |
| `Set_Error_*` | Set variable | One per stage, including the semantic model refresh and the metadata logging step. Writes a readable message into `error_message` when that stage fails |
| `Stored_Procedure_Insert_Metadata` | Stored procedure | `Returns_Control.Insert_Pipeline_Metadata` logs the run |
| `If_Check_Error` | If condition | If `error_message` is not empty, send the failure email and fail the pipeline; otherwise send the success email |

Both notebooks run with the session tag `returns_load`, so the second notebook can reuse the Spark session started by the first. All main activities use a 15 minute timeout and 1 retry with a 30 second interval; the semantic model refresh has a 20 minute timeout and the same retry.

## Watermark logic

- `Returns_Control.Pipeline_Watermark` holds one row per pipeline name with the last month successfully loaded.
- The watermark moves only at the very end, after the Gold merge succeeds. A failed run leaves it unchanged, so the next run retries the same month.
- `Update_Watermark` only updates when the new month is later than the stored one (`year*100+month` comparison). Reprocessing an old month therefore never moves the watermark backwards.
- The semantic model refresh runs after the watermark moves. If it fails, Gold and the watermark are already updated: the run is logged as **Failed** and emailed, but the next run loads the following month. Refresh the model manually (or reprocess the month) to update the report.
- A missing source file (HTTP 404 / "not found") is reported as `Source file not found for <year>-<month>. Watermark not advanced.` and logged with status **No Source File**. This is the normal state when the next month has not been published yet.

## Data layers

### Bronze

The Copy activity reads the CSV (comma delimited, header row, quote and escape characters set) and writes Parquet with snappy compression into a year/month folder. Types are not enforced here; amounts such as `₹3,355.27` stay as text.

### Silver

`Bronze_Silver_Notebook_Optimised`:

1. Validates the `p_year` and `p_month` parameters.
2. Reads the Bronze month. A missing or unreadable path raises an error. An empty month exits with 0 rows.
3. Checks the structure of the file. A missing mandatory column or a new unexpected column (schema drift) raises an error that names the columns and fails the run before anything is loaded. Any other missing column is filled with NULL.
4. Removes exact duplicate rows, cleans and types the data, and runs the row-level validation rules.
5. Writes rejected rows to quarantine and merges valid rows into the Silver Delta table on `Order_Line_ID` (update matched, insert new). The first ever load creates the table partitioned by `Order_Year` and `Order_Month`.

Full rule list: [BUSINESS_RULES.md](BUSINESS_RULES.md).

### Gold (staging then final)

`Silver_Gold_Notebook_Optimised` reads the month from Silver, validates it, splits it into one fact and five dimension data frames and overwrites the matching `*_stg` tables in the Warehouse. After each write it reads the table back and compares the row count with the expected count; a mismatch fails the notebook. `Merge_Returns_Gold_Final` then upserts the staging tables into Gold inside a single transaction (all-or-nothing, rolled back on error).

## Warehouse design

Star schema in `Returns_Gold`:

| Table | Grain / key | Columns |
|---|---|---|
| `Fact_Orders` | One row per order line, key `Order_Line_ID` | Order, customer, product, channel and payment references, `Quantity`, `Unit_Price`, `Unit_Cost`, `Discount_Pct`, `Shipping_Fee`, `Delivery_Date`, return fields (`Return_Flag`, `Return_Date`, `Return_Reason`, `Return_Quantity`, `Refund_Deduction_Pct`), `Product_Rating`, `Order_Year`, `Order_Month` |
| `Dim_Product` | `Product_ID` | `Product_Name`, `Category`, `Subcategory`, `Brand` |
| `Dim_Customer` | `Customer_ID` | `Customer_Segment`, `Customer_City`, `Customer_State` |
| `Dim_Channel` | `Sales_Channel` | |
| `Dim_Payment_Method` | `Payment_Method` | |
| `Dim_Return_Reason` | `Return_Reason` | |
| `Dim_Date` | `Date` | `Year`, `Month_Number`, `Month`, `Year_Month`. One row per day, 2020-01-01 to 2035-12-31. Created once by `Warehouse_Setup/10_Create_Dim_Date.sql`; not touched by the pipeline, staging or the Gold merge |

The model uses natural business keys (no surrogate keys) and joins on those values. `Returns_Staging` mirrors the fact and the five loaded dimensions with a `_stg` suffix (there is no `Dim_Date_stg`).

The Power BI semantic model sits on top of these tables in Direct Lake mode. See [SEMANTIC_MODEL.md](SEMANTIC_MODEL.md) for its relationships and measures and [POWER_BI_REPORT.md](POWER_BI_REPORT.md) for the report.

`Returns_Control`:

| Table / procedure | Purpose |
|---|---|
| `Pipeline_Watermark` | Last processed year and month, last successful run ID, last updated time |
| `Pipeline_Metadata` | One row per run: name, run ID, trigger type, processed year/month and month name, source file name, Bronze / Silver / Gold-staging / Gold-final row counts, start and end time, duration in seconds, load status, error message |
| `Update_Watermark` | Moves the watermark forward only |
| `Insert_Pipeline_Metadata` | Writes the run log. Idempotent on `Pipeline_Run_ID`, so a retry does not create a second row. Accepts error text up to 8,000 characters and stores the first 2,000, so a long error cannot make logging fail |

## Error handling and logging

- Every stage has its own `Set_Error_*` activity that runs on failure and stores a message such as `Bronze_Silver_Notebook Failed: <error>`. This includes the semantic model refresh (`Ecommerce_Returns_Sematic_Model Failed: <error>`).
- `Insert_Pipeline_Metadata` runs whether the stages succeeded or failed, and derives the status: **Success**, **No Source File**, or **Failed**.
- If `Insert_Pipeline_Metadata` itself fails, `Set_Error_Stored_Procedure_Insert_Metadata` stores that error. `If_Check_Error` depends on this activity (**Succeeded** or **Skipped**), which always completes, so an email is sent even if logging fails. On error it emails and fails the pipeline with the code `PIPELINE_STAGE_FAILED`, so the run shows as failed in Fabric Monitor. Otherwise it emails a success summary.
- Row-level problems are not pipeline failures. They are quarantined and the run continues; the quarantined count is printed in the notebook output.
- **Reject-rate limit.** If more than `p_max_reject_pct` (default 5) percent of a month's rows are quarantined, the notebook fails before loading (status `Rejected` in its audit table), so the failure email is sent and the watermark does not advance.
- **Notebook audit table.** At the end of each run, `Bronze_Silver_Notebook_Optimised` and `Silver_Gold_Notebook_Optimised` each append one row to their own Lakehouse Delta table (`audit_bronze_silver` and `audit_silver_gold`): notebook, layer, year and month, status (`Success`, `No Data` or `Rejected`), start, end and duration, input, valid, quarantined and written row counts, rows inserted and updated (Bronze to Silver only), source, target and quarantine paths, list of missing (NULL-filled) columns, Spark application ID and, if the pipeline passes it as `p_run_id`, the pipeline run ID. Writing the audit row never fails a notebook, and a run that fails earlier is recorded by `Pipeline_Metadata` instead.
- Pipeline and structural problems (missing Silver table, missing required Silver column, staging row-count mismatch, invalid parameters) raise errors and fail the stage.

Email setup: [EMAIL_NOTIFICATIONS.md](EMAIL_NOTIFICATIONS.md).

## Project structure

```
Fabric-Ecommerce-Return-Analytics/
├── README.md                                  Project overview
├── Warehouse_Setup/                           Numbered T-SQL scripts (schemas, tables, procedures, watermark seed, Dim_Date calendar, reset)
├── apps_script/Code.gs                        Google Apps Script that sends the pipeline emails
├── LICENSE                                    MIT
├── docs/
│   ├── PIPELINE_ARCHITECTURE.md               This file
│   ├── BUSINESS_RULES.md                      Cleaning, validation and merge rules
│   ├── EMAIL_NOTIFICATIONS.md                 Success/failure email setup
│   ├── SEMANTIC_MODEL.md                      Direct Lake semantic model reference
│   ├── POWER_BI_REPORT.md                     Report pages and visuals
│   └── SCHEMA_CHECK_DEMO.md                   Schema drift / missing column demo
├── Data/
│   ├── Ecommerce_Returns_Refunds_2025_01_January.csv ... _2025_12_December.csv
│   └── Ecommerce_Returns_Refunds_2024_01_January.csv, _2024_02_February.csv   Demo files that fail the schema checks on purpose
└── Fabric_Ecommerce_Return_Analysis/          Fabric Git integration root
    ├── Readme.md                              Auto-created by Fabric
    ├── Ecommerce_Returns_Lakehouse.Lakehouse/ Lakehouse definition
    ├── Ecommerce_Returns_Pipeline.DataPipeline/
    │   ├── .platform
    │   └── pipeline-content.json              All activities, variables and parameters
    ├── Notebooks/
    │   ├── Bronze_Silver_Notebook_Optimised.Notebook/   Used by the pipeline
    │   ├── Silver_Gold_Notebook_Optimised.Notebook/     Used by the pipeline
    │   ├── Bronze_Silver_Notebook.Notebook/             Earlier, simpler version (not used by the pipeline)
    │   └── Silver_Gold_Notebook.Notebook/               Earlier, simpler version (not used by the pipeline)
    ├── Ecommerce_Returns_Semantic_Model.SemanticModel/   Direct Lake model (TMDL): tables, relationships, measures
    ├── Ecommerce_Returns_Report.Report/           Power BI report (PBIR), bound to the semantic model
    └── Ecommerce_Returns_Warehouse.Warehouse/
        ├── Ecommerce_Returns_Warehouse.sqlproj
        ├── Security/                          Schema definitions
        ├── Returns_Control/
        │   ├── Tables/                        Pipeline_Watermark, Pipeline_Metadata
        │   └── StoredProcedures/              Update_Watermark, Insert_Pipeline_Metadata
        ├── Returns_Staging/
        │   ├── Tables/                        Fact_Orders_stg, Dim_*_stg
        │   └── StoredProcedures/              Merge_Returns_Gold_Final
        └── Returns_Gold/
            └── Tables/                        Fact_Orders, Dim_Product, Dim_Customer,
                                               Dim_Channel, Dim_Payment_Method, Dim_Return_Reason,
                                               Dim_Date (calendar, not loaded by the pipeline)
```

The pipeline references the Optimised notebooks by ID. The two earlier notebooks are kept for comparison and are not run.

## Setup notes

1. Create the workspace items from this repo (Fabric Git integration or manual import): Lakehouse, Warehouse, notebooks, pipeline.
2. Set the default Lakehouse on both notebooks to `Ecommerce_Returns_Lakehouse`.
3. In the pipeline, recreate the connections: the HTTP connection used by the Copy activity (base URL `https://raw.githubusercontent.com/<github-user>/`; the activity appends `Fabric-Ecommerce-Return-Analytics/refs/heads/main/Data/<file>.csv`, so the source files are read from the `main` branch of that repository), the Lakehouse and Warehouse references, the email Web connection (see the email guide), and the Power BI connection used by the semantic model refresh activity (select your own workspace and `Ecommerce_Returns_Semantic_Model` there).
4. Build the warehouse objects by running the scripts in [`Warehouse_Setup`](../Warehouse_Setup/README.md) in order. `05_Seed_Watermark.sql` creates the starting watermark row: set it to the month **before** the first month to load (for January 2025, that is 2024 and 12). Run `10_Create_Dim_Date.sql` as well; the semantic model needs the calendar table.

5. Open the semantic model once and confirm its Direct Lake source points at your `Ecommerce_Returns_Warehouse`, and that the report is bound to it.
6. Run the pipeline. Each run loads one month and then refreshes the semantic model; schedule it to run on the cadence you want.
