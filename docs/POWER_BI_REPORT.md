# Power BI Report

Reference for `Ecommerce_Returns_Report`, the Power BI report on top of the [semantic model](SEMANTIC_MODEL.md). It answers three questions: how sales are doing, how returns and refunds behave, and which customers and products drive them.

## Contents

1. [Overview](#1-overview)
2. [Shared filters](#2-shared-filters)
3. [Page 1: Overview](#3-page-1-overview)
4. [Page 2: Returns analysis](#4-page-2-returns-analysis)
5. [Page 3: Customers and products](#5-page-3-customers-and-products)
6. [Reading the numbers](#6-reading-the-numbers)
7. [Repository files](#7-repository-files)
8. [Working with the report](#8-working-with-the-report)

## 1. Overview

| Item | Value |
|---|---|
| Name | `Ecommerce_Returns_Report` |
| Data | `Ecommerce_Returns_Semantic_Model` (Direct Lake), bound by relative path |
| Pages | 3, each 1280 x 770, fit to page |
| Layout | Title band, 4 slicers, 6 KPI cards (the new card visual), then charts |
| Theme | Custom blue theme `Retail Blue` (on the Fluent 2 base theme): blue title band, light blue canvas, white bordered tiles, blue data colours with one yellow accent |
| Freshness | The pipeline refreshes the model at the end of every run, so the report shows the latest loaded month |

Every page opens with a title bar (`E-Commerce Returns & Refund Analytics | <page name>`, a text box). Below it every page uses the same rows (filters, then six cards), so a reader can move between pages without relearning the layout.

## 2. Shared filters

Each page has the same four slicers. Slicers on one page do not carry to the others, so reapply a filter after changing page.

| Slicer | Field |
|---|---|
| Category | `Dim_Product[Category]` |
| Sales channel | `Dim_Channel[Sales_Channel]` |
| Month | `Dim_Date[Year Month]` |
| Payment method | `Dim_Payment_Method[Payment_Method]` |

Visuals on a page filter each other when a value is selected (cross-filtering is on by default). Measures are defined in [SEMANTIC_MODEL.md](SEMANTIC_MODEL.md#5-measures).

## 3. Page 1: Overview

**Question:** How are sales and returns doing overall?

| Visual | Type | Measure and field |
|---|---|---|
| Net Sales | Card | `Net Sales` |
| Orders | Card | `Orders` |
| Avg Order Value | Card | `Avg Order Value` |
| Return Rate (Lines) | Card | `Return Rate (Lines)` |
| Refund Amount | Card | `Refund Amount` |
| Gross Margin % | Card | `Gross Margin %` |
| Net Sales and Refund Amount by month | Line chart | `Net Sales` and `Refund Amount by Return Date` by `Dim_Date[Year Month]` |
| Net Sales by Category | Bar chart | `Net Sales` by `Dim_Product[Category]` |
| Net Sales by sales channel | Column chart | `Net Sales` by `Dim_Channel[Sales_Channel]` |
| Net Sales by payment method | Donut chart | `Net Sales` by `Dim_Payment_Method[Payment_Method]` |

The line chart places refunds in the month they were returned, not the month of the order, so it shows cash going out against sales coming in.

## 4. Page 2: Returns analysis

**Question:** How much comes back, why, and where?

| Visual | Type | Measure and field |
|---|---|---|
| Returned Lines | Card | `Returned Lines` |
| Returned Units | Card | `Returned Units` |
| Return Rate (Units) | Card | `Return Rate (Units)` |
| Refund Amount | Card | `Refund Amount` |
| Refund % of Net Sales | Card | `Refund % of Net Sales` |
| Avg Days to Return | Card | `Avg Days to Return` |
| Refund amount by return reason | Bar chart | `Refund Amount` by `Dim_Return_Reason[Return_Reason]` |
| Return rate by category | Bar chart | `Return Rate (Lines)` by `Dim_Product[Category]` |
| Return rate by month | Line chart | `Return Rate (Lines)` by `Dim_Date[Year Month]` |
| Return rate by sales channel | Column chart | `Return Rate (Lines)` by `Dim_Channel[Sales_Channel]` |

Here the refund amount follows the **order** month (`Refund Amount`), so a return rate by month compares returns with the orders they came from.

## 5. Page 3: Customers and products

**Question:** Who buys, what sells, and which products cost the most in refunds?

| Visual | Type | Measure and field |
|---|---|---|
| Customers | Card | `Customers` |
| Units Sold | Card | `Units Sold` |
| Gross Profit | Card | `Gross Profit` |
| Gross Margin % | Card | `Gross Margin %` |
| Avg Rating | Card | `Avg Rating` |
| Avg Days to Deliver | Card | `Avg Days to Deliver` |
| Net Sales by state | Bar chart | `Net Sales` by `Dim_Customer[Customer_State]` |
| Net Sales by customer segment | Bar chart | `Net Sales` by `Dim_Customer[Customer_Segment]` |
| Products by refund amount | Table | `Dim_Product[Product_Name]`, `Dim_Product[Brand]`, `Net Sales`, `Return Rate (Lines)`, `Refund Amount` |

Sort the table by `Refund Amount` to find the products worth investigating first, then use the slicers to see whether the problem sits in one category, channel or month.

## 6. Reading the numbers

- **Months are order months** unless a visual says otherwise. Only the Overview line chart uses the return month.
- **Net Sales** is gross sales less discounts, before refunds. **Gross Profit** is after refunds and the cost of the units that were kept.
- **A return rate is a share of order lines** (or of units, where the card says so), not of revenue. Use `Refund % of Net Sales` for the money view.
- **Empty visuals** usually mean a month has not been loaded yet, or an order or return date falls outside the `Dim_Date` range (2020 to 2035).
- **Currency** is rupees, as in the source files.
- The dataset is the 2025 monthly files loaded by the pipeline, one month per run, so the report fills up as months are processed.

## 7. Repository files

The report is stored in PBIR format under `Fabric_Ecommerce_Return_Analysis/Ecommerce_Returns_Report.Report/`:

| File | Content |
|---|---|
| `definition.pbir` | Report definition and the link to the semantic model (`../Ecommerce_Returns_Semantic_Model.SemanticModel`) |
| `definition/report.json` | Report-level settings and theme registration |
| `definition/pages/pages.json` | Page order (Overview, Returns analysis, Customers and products) |
| `definition/pages/<page id>/page.json` | One page: name and size |
| `definition/pages/<page id>/visuals/<visual id>/visual.json` | One visual: type, position, fields and formatting |
| `StaticResources/SharedResources/` | Base theme files |
| `StaticResources/RegisteredResources/Retail_Blue_Theme.json` | The custom blue theme: colour palette, tile and title styling, card, slicer, chart axis and table styles |

Each visual is its own small file, so a change to one chart shows up as a small, readable diff in Git.

## 8. Working with the report

- **Edit** in the Fabric workspace (the report editor) or open the PBIR folder in Power BI Desktop developer mode. Commit through Git integration.
- **New visuals** should use the measures, not raw columns. Implicit measures are discouraged in the model, and the foreign-key columns on `Fact_Orders` are hidden.
- **Change the look** in the theme file (or **View > Themes** in the editor) rather than formatting visuals one by one. Visuals carry no hard-coded colours, so a theme change restyles the whole report.
- **Add a page** by copying an existing one so the slicer row and card row line up.
- **Rebinding:** `definition.pbir` points to the model by relative path. Keep the report and the model in the same parent folder, or update that path.
- **Sharing:** viewers need access to the workspace (or an app built from it) and read access to the semantic model. Because the model uses Direct Lake, they also need permission on the underlying warehouse data unless the model is set to a fixed identity.
