# Business Rules

This document lists every rule the pipeline applies to the returns data. Rules live in the two notebooks and the Warehouse stored procedures; this file is the plain-language reference.

**Principle:** a row that breaks a rule is **quarantined**, not deleted and not allowed to stop the run. A row can break several rules at once and all reasons are recorded. Only structural problems (a missing file, missing required columns, invalid parameters, a staging count mismatch) fail the pipeline.

## Contents

1. [Run-level rules](#1-run-level-rules)
2. [Bronze to Silver: cleaning](#2-bronze-to-silver-cleaning)
3. [Bronze to Silver: validation](#3-bronze-to-silver-validation)
4. [Silver to Gold: validation](#4-silver-to-gold-validation)
5. [Quarantine](#5-quarantine)
6. [Gold merge rules](#6-gold-merge-rules)
7. [Run status rules](#7-run-status-rules)
8. [Allowed values](#8-allowed-values)

## 1. Run-level rules

| # | Rule |
|---|---|
| 1.1 | The pipeline loads **one calendar month per run**. |
| 1.2 | With no parameters, the month is the one after the watermark. December is followed by January of the next year. |
| 1.3 | A manual reprocess needs **both** `reprocess_year` and `reprocess_month`. If either is empty, both are ignored. A one-digit month is padded (`3` becomes `03`). |
| 1.4 | An invalid reprocess month or year fails the run with `Set_watermark_month_name Failed: invalid reprocess input`. |
| 1.5 | The watermark only moves forward, and only after the Gold merge succeeds. |
| 1.6 | Source file missing for the month: the run is logged as **No Source File** and the watermark does not move. The pipeline still ends as failed and sends the failure email, so a scheduled run before the next file is published will alert. |
| 1.7 | The pipeline runs one instance at a time. |
| 1.8 | Notebook parameters: `p_year` must be four digits, `p_month` must be 1 to 12, otherwise the notebook raises an error. |

## 2. Bronze to Silver: cleaning

Applied before validation, in this order.

| # | Rule |
|---|---|
| 2.1 | **Exact duplicate rows** are removed. |
| 2.2 | **Amounts** (`Unit_Price`, `Unit_Cost`, `Discount_Pct`, `Shipping_Fee`, `Refund_Deduction_Pct`): everything except digits, `.` and `-` is removed (currency sign, thousands commas, `%`), then cast to decimal. Prices, cost and shipping are `decimal(12,2)`; percentages are `decimal(6,2)`. |
| 2.3 | **Dates** (`Order_Date`, `Delivery_Date`, `Return_Date`) are parsed from `dd-MMM-yyyy`, for example `27-Jan-2025`. |
| 2.4 | **Integers** (`Quantity`, `Return_Quantity`, `Product_Rating`) are trimmed and cast to int. |
| 2.5 | **Text standardization:** `Sales_Channel`, `Payment_Method` and `Return_Reason` are trimmed and converted to Title Case (so `MARKETPLACE` becomes `Marketplace`, `wallet` becomes `Wallet`, `UPI` becomes `Upi`). |
| 2.6 | `COD` / `Cod` in `Payment_Method` becomes `Cash On Delivery`. |
| 2.7 | `Not As Expected` in `Return_Reason` becomes `Product Not As Expected`. |
| 2.8 | **Return flag:** `Yes`, `Y`, `Returned` (any case) become `Yes`. Every other value becomes `No`. The original text is still checked in rule 3.9. |
| 2.9 | `Order_Year` and `Order_Month` are derived from `Order_Date`. |
| 2.10 | A source column that is missing is added as NULL (and then fails the mandatory check if it is a mandatory column). A Silver column missing from the source is also added as NULL. |
| 2.11 | **Schema drift:** a new source column that is not in Silver and not expected is logged as a warning, left out of Silver, and any row that has a value in it is quarantined. |

## 3. Bronze to Silver: validation

A row failing any of these is quarantined.

**Mandatory fields.** Blank or missing is a failure for: `Order_Line_ID`, `Order_ID`, `Order_Date`, `Customer_ID`, `Product_ID`, `Quantity`, `Unit_Price`, `Sales_Channel`, `Payment_Method`.

**Type check.** A value was supplied but could not be converted (bad date, non-numeric quantity or amount) for any date, integer or amount column.

**ID formats**

| Field | Pattern | Example |
|---|---|---|
| `Order_Line_ID` | `OL-` followed by 6 or more digits | `OL-000022` |
| `Order_ID` | `ORD-` + 4-digit year + `-` + 5 or more digits | `ORD-2025-00019` |
| `Customer_ID` | `C` followed by 4 or more digits | `C0666` |
| `Product_ID` | `P` followed by 3 or more digits | `P050` |

**Allowed values** (after standardization, case-insensitive): see [section 8](#8-allowed-values) for `Customer_Segment`, `Category`, `Sales_Channel`, `Payment_Method`, `Return_Reason`.

**Text quality.** `Customer_City`, `Customer_State`, `Product_Name`, `Subcategory`, `Brand`, when present, must start with a letter or digit and be 2 to 100 characters using only letters, digits, spaces and `& ' . , ( ) / + -`.

**Category consistency.** `Subcategory` must belong to its `Category` (mapping in section 8).

**Returns consistency**

| # | Rule |
|---|---|
| 3.9 | `Return_Flag` must be one of `yes`, `y`, `returned`, `no`, `n`, `not returned` (any case) when supplied. |
| 3.10 | `Return_Flag = Yes` requires a `Return_Date`, a `Return_Reason`, and a `Return_Quantity` greater than 0. |
| 3.11 | `Return_Flag = No` must not have a `Return_Date`, a `Return_Reason`, or a `Return_Quantity` greater than 0. |
| 3.12 | `Return_Quantity` cannot be negative and cannot exceed `Quantity`. |

**Dates**

| # | Rule |
|---|---|
| 3.13 | `Order_Date` and `Return_Date` cannot be in the future. |
| 3.14 | `Delivery_Date` cannot be before `Order_Date`. |
| 3.15 | `Return_Date` cannot be before `Order_Date` or before `Delivery_Date`. |
| 3.16 | `Order_Date` must fall in the requested year and month. A row from another month in the file is quarantined. |

**Numeric ranges**

| Field | Rule |
|---|---|
| `Quantity` | Greater than 0 |
| `Unit_Price`, `Unit_Cost`, `Shipping_Fee` | Not negative |
| `Discount_Pct`, `Refund_Deduction_Pct` | 0 to 100 |
| `Product_Rating` | 1 to 5 (optional field; blank is allowed) |

**Duplicate keys in the batch.** If the same `Order_Line_ID` appears more than once, one row is kept (a clean row first, then the one with the latest `Return_Date`, then the latest `Delivery_Date`). The others are quarantined with `Duplicate Order_Line_ID in batch`. This also prevents the Silver merge from failing on duplicate keys.

## 4. Silver to Gold: validation

The Silver table is trusted but checked again, because Gold is what reporting uses.

| # | Rule |
|---|---|
| 4.1 | The Silver table must exist and contain all required columns. If not, the notebook fails (a contract break, not bad data). |
| 4.2 | Only the requested `Order_Year` / `Order_Month` is processed. |
| 4.3 | Mandatory for the fact table: `Order_Line_ID`, `Order_ID`, `Order_Date`, `Product_ID`, `Customer_ID`, `Sales_Channel`, `Payment_Method`. |
| 4.4 | `Quantity` must be greater than 0 and `Unit_Price` must be present and not negative. |
| 4.5 | Same range rules as 3: return quantity, cost, shipping, discount %, refund deduction %, rating 1 to 5. |
| 4.6 | `Delivery_Date` and `Return_Date` cannot be before `Order_Date`. |
| 4.7 | `Return_Flag` must be exactly `Yes` or `No`. `Yes` needs a `Return_Reason` and a `Return_Date`. |
| 4.8 | Text quality and Subcategory-to-Category checks repeat from section 3. |
| 4.9 | One row per `Order_Line_ID`; extras are quarantined with `Duplicate Order_Line_ID in silver`. |
| 4.10 | Dimensions use only valid fact rows, with one row per key. If the same `Product_ID` or `Customer_ID` has conflicting attributes inside the batch, a warning is printed and one version is kept. |
| 4.11 | `Return_Reason` dimension values exclude NULL. |
| 4.12 | **Load check:** after writing each staging table, the row count is read back and compared with the expected count. A mismatch fails the notebook. |
| 4.13 | An empty month still runs: staging tables are overwritten with zero rows so stale data from the previous month does not get merged again. |

## 5. Quarantine

| Item | Detail |
|---|---|
| Location | Bronze to Silver: `Files/quarantine/ecommerce_returns/bronze_silver/year=YYYY/month=MM/`. Silver to Gold: `.../silver_gold/year=YYYY/month=MM/` |
| Content | The original row, a `Validation_Errors` column listing every failed rule separated by `; `, and `Quarantined_At`. Bronze to Silver rows keep the **original raw values** (before cleaning), plus `Source_Year` and `Source_Month`. |
| Reruns | The month's quarantine is **overwritten**, so reprocessing does not duplicate rows. |
| Visibility | The notebook prints the total and a count per rule. |
| Follow-up | Fix the source file or the rule, then reprocess the month with the pipeline parameters. |

Quarantined rows are **not** included in the Silver or Gold counts.

## 6. Gold merge rules

`Returns_Staging.Merge_Returns_Gold_Final` runs these merges in order:

| Table | Key | When matched | When not matched |
|---|---|---|---|
| `Dim_Product` | `Product_ID` | Update name, category, subcategory, brand | Insert |
| `Dim_Customer` | `Customer_ID` | Update segment, city, state | Insert |
| `Dim_Channel` | `Sales_Channel` | Nothing | Insert |
| `Dim_Return_Reason` | `Return_Reason` | Nothing | Insert |
| `Dim_Payment_Method` | `Payment_Method` | Nothing | Insert |
| `Fact_Orders` | `Order_Line_ID` | Update all columns | Insert |

All six merges run inside **one transaction**: if any of them fails, everything is rolled back, Gold stays exactly as it was, and the error is re-raised so the pipeline fails with the real message.

Consequences: rerunning a month corrects existing rows and adds new ones without duplicating; dimension attributes (product, customer) always reflect the latest load; nothing is ever deleted from Gold by the pipeline.

## 7. Run status rules

Written to `Returns_Control.Pipeline_Metadata.Load_Status`:

| Status | When |
|---|---|
| **Success** | `error_message` is empty. |
| **No Source File** | The error message starts with `Source file not found`. |
| **Failed** | Any other error message. |

The error text is stored up to 2,000 characters (longer messages are cut). Also recorded: row counts for Bronze (rows copied), Silver (valid rows merged), Gold staging (fact rows staged) and the Gold fact table total after the merge, plus start time, end time and duration in seconds. A run ID is logged only once, so a retried procedure does not create a duplicate row.

Notebook counts: Silver counts **valid** rows only, so `Bronze rows - Silver rows` is approximately the quarantined rows plus exact duplicates removed.

## 8. Allowed values

| Field | Allowed values |
|---|---|
| `Customer_Segment` | Consumer, Corporate, Home Office |
| `Category` | Fashion, Electronics, Home & Kitchen, Beauty, Sports, Books |
| `Sales_Channel` | Website, Mobile App, Marketplace, Offline Store |
| `Payment_Method` | Upi, Credit Card, Debit Card, Cash On Delivery, Wallet, Net Banking, Cash |
| `Return_Reason` | Wrong Size, Damaged Product, Product Not As Expected, Quality Issue, Wrong Item, Defective Product, Late Delivery, Changed Mind |
| `Return_Flag` (source spellings) | yes, y, returned, no, n, not returned |

**Subcategory by Category**

| Category | Subcategories |
|---|---|
| Beauty | Personal Care, Skin Care |
| Books | Books |
| Electronics | Mobile Accessories, Smart Devices, Audio |
| Fashion | Men's Clothing, Women's Clothing, Footwear |
| Home & Kitchen | Home Furnishing, Kitchen |
| Sports | Fitness, Outdoor |

To add a new category, channel, payment method or reason, update the lists at the top of `Bronze_Silver_Notebook_Optimised` (`ALLOWED_VALUES`, `SUBCATEGORY_BY_CATEGORY`) first; otherwise rows with the new value are quarantined.
