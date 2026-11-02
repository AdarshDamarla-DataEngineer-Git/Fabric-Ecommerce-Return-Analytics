# Schema Check Demo (2024 files)

Two demo source files show that `Bronze_Silver_Notebook_Optimised` detects a structural problem in the source file, stops before loading anything, and triggers the failure alert. They sit in `Data/` with the same name pattern as the real files, so the Copy activity picks them up without any change to the pipeline.

They use the year **2024** (file names and dates), so they never collide with the real 2025 data.

| File | Month | What is wrong | Expected error |
|---|---|---|---|
| `Data/Ecommerce_Returns_Refunds_2024_01_January.csv` | 2024-01 | Extra column `Loyalty_Tier` (filled in every row) that the notebook does not expect: **schema drift** | `Schema drift: new column(s) in the source file: ['Loyalty_Tier']` |
| `Data/Ecommerce_Returns_Refunds_2024_02_February.csv` | 2024-02 | Mandatory column `Customer_ID` removed | `Mandatory column(s) missing from the source file: ['Customer_ID']` |

Apart from that one column, the files are the January and February 2025 data shifted to 2024 (about 296 and 271 rows). Dates run from `...-2025` to `...-2024` and order IDs from `ORD-2025-` to `ORD-2024-`, so everything else would pass validation. The failure is purely structural.

## Running the demo

Use the pipeline's backfill parameters, one run per file:

| Run | `reprocess_year` | `reprocess_month` |
|---|---|---|
| Schema drift | `2024` | `1` |
| Missing mandatory column | `2024` | `2` |

For each run:

1. `Copy_Github_Bronze` succeeds: the Copy activity does not check the schema, so the file lands in Bronze as it is (`bronze/ecommerce_returns/year=2024/month=01/`).
2. `Bronze_Silver_Notebook` fails in its schema check, before anything is quarantined or merged into Silver. The error names the column.
3. The `Set_Error_*` activity captures the message, `Insert_Pipeline_Metadata` logs the run as **Failed**, and `If_Check_Error` sends the `[FAILED] Ecommerce_Returns_Pipeline` email and fails the pipeline.
4. The email shows `Processing: 2024-January` (or `2024-February`) with the Bronze row count and the error text.

The files are read from the **`main`** branch on GitHub, so merge them to `main` (or point the HTTP connection at the branch that has them) before the demo.

## What does not change

- **The watermark.** It only moves forward after a successful Gold merge, and a failed run never moves it, so the demo does not disturb the 2025 load. A reprocess of an older month cannot move it backwards either.
- **Silver, Gold and the semantic model.** The notebook raises before loading, so no downstream step runs and the semantic model is not refreshed.
- **The real data.** The 2025 files are untouched.

The Bronze Parquet folders for 2024 stay in the Lakehouse after the demo. They are harmless, and are overwritten if the same month is copied again.

## Related rules

Rules 2.10 (missing columns) and 2.11 (schema drift) in [BUSINESS_RULES.md](BUSINESS_RULES.md#2-bronze-to-silver-cleaning). A missing column that is **not** mandatory (for example `Product_Rating`) does not fail the run: it is filled with NULL and listed in the notebook audit row.
