# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c6e9196c-35d7-41b5-a016-530c6ffcaca5",
# META       "default_lakehouse_name": "Ecommerce_Returns_Lakehouse",
# META       "default_lakehouse_workspace_id": "06435289-d1b3-4645-b600-05705664d90b",
# META       "known_lakehouses": [
# META         {
# META           "id": "c6e9196c-35d7-41b5-a016-530c6ffcaca5"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

p_year=""
p_month=""
p_run_id=""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Silver → Gold staging
# 
# 
# Reads one month of validated Silver data, applies a final set of checks, and loads the fact and dimension **staging** tables in the Warehouse. The Gold merge itself runs afterwards in the pipeline (`Returns_Staging.Merge_Returns_Gold_Final`).
# 
# | | |
# |---|---|
# | **Input** | `Files/silver/ecommerce_returns/` (Delta), filtered to `Order_Year = p_year` and `Order_Month = p_month` |
# | **Output** | `Ecommerce_Returns_Warehouse.Returns_Staging`: `Fact_Orders_stg`, `Dim_Product_stg`, `Dim_Customer_stg`, `Dim_Channel_stg`, `Dim_Return_Reason_stg`, `Dim_Payment_Method_stg` (overwritten every run) |
# | **Quarantine** | `Files/quarantine/ecommerce_returns/silver_gold/year=<p_year>/month=<p_month>/` (Delta, rejected rows + reasons) |
# | **Audit** | one row appended to the Lakehouse table `notebook_audit` |
# | **Exit value** | number of rows written to `Fact_Orders_stg` |
# 
# **Parameters** (first cell): `p_year`, `p_month`, `p_run_id` (`@pipeline().RunId` to link audit rows to the pipeline run).
# 
# **Flow:** validate parameters → read Silver → check columns → validate rows → de-duplicate keys → split valid / quarantine → build fact + dimensions → write staging with read-back check → audit → exit.


# MARKDOWN ********************

# ### 1. Setup

# CELL ********************

import re
from datetime import datetime, timezone

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from pyspark import StorageLevel
from delta.tables import DeltaTable
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

spark.conf.set("spark.sql.ansi.enabled", "false")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Configuration

# CELL ********************

# Paths and targets
sourceFilePath = "Files/silver/ecommerce_returns/"
quarantinePath = f"Files/quarantine/ecommerce_returns/silver_gold/year={p_year}/month={p_month}/"
Target_Warehouse = "Ecommerce_Returns_Warehouse"
Target_Schema = "Returns_Staging"

# Columns the Silver table must contain (a missing one means the Silver contract was broken upstream)
REQUIRED_COLS = [
    "Order_Line_ID", "Order_ID", "Order_Date", "Order_Year", "Order_Month", "Delivery_Date",
    "Quantity", "Return_Quantity", "Unit_Price", "Unit_Cost", "Discount_Pct", "Shipping_Fee",
    "Return_Flag", "Return_Date", "Refund_Deduction_Pct", "Product_Rating",
    "Product_ID", "Product_Name", "Category", "Subcategory", "Brand",
    "Customer_ID", "Customer_Segment", "Customer_City", "Customer_State",
    "Sales_Channel", "Payment_Method", "Return_Reason",
]

# Blank values here are quarantined (keys only; other columns may be blank)
FACT_KEY_COLS = [
    "Order_Line_ID", "Order_ID", "Order_Date", "Product_ID", "Customer_ID",
    "Sales_Channel", "Payment_Method",
]

# Free-text columns must look like real names (2-100 chars)
TEXT_PATTERN_COLS = ["Customer_City", "Customer_State", "Product_Name", "Subcategory", "Brand"]
TEXT_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9 &'.,()/+\-]{1,99}$"

# Valid Subcategory for each Category (compared case-insensitively)
SUBCATEGORY_BY_CATEGORY = {
    "Beauty": ["Personal Care", "Skin Care"],
    "Books": ["Books"],
    "Electronics": ["Mobile Accessories", "Smart Devices", "Audio"],
    "Fashion": ["Men's Clothing", "Women's Clothing", "Footwear"],
    "Home & Kitchen": ["Home Furnishing", "Kitchen"],
    "Sports": ["Fitness", "Outdoor"],
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. Audit logging

# CELL ********************

# ---------------- Audit logging ----------------
# One row per notebook run is appended to this Lakehouse Delta table (Tables section).
# It is separate from the Bronze -> Silver audit table (audit_bronze_silver), so neither notebook touches the other's table.
# A failed run raises before the audit write and is recorded by the pipeline in Returns_Control.Pipeline_Metadata instead.
AUDIT_TABLE = "audit_silver_gold"
NOTEBOOK_NAME = "Silver_Gold_Notebook_Optimised"
AUDIT_LAYER = "silver_to_gold_staging"
run_start_time = datetime.now(timezone.utc)

AUDIT_SCHEMA = StructType([
    StructField("audit_time_utc", TimestampType()),
    StructField("pipeline_run_id", StringType()),
    StructField("notebook_name", StringType()),
    StructField("layer", StringType()),
    StructField("process_year", IntegerType()),
    StructField("process_month", IntegerType()),
    StructField("status", StringType()),
    StructField("start_time_utc", TimestampType()),
    StructField("end_time_utc", TimestampType()),
    StructField("duration_seconds", LongType()),
    StructField("silver_rows_read", LongType()),
    StructField("valid_rows", LongType()),
    StructField("quarantined_rows", LongType()),
    StructField("fact_orders_rows", LongType()),
    StructField("dim_product_rows", LongType()),
    StructField("dim_customer_rows", LongType()),
    StructField("dim_channel_rows", LongType()),
    StructField("dim_return_reason_rows", LongType()),
    StructField("dim_payment_method_rows", LongType()),
    StructField("source_path", StringType()),
    StructField("target_schema", StringType()),
    StructField("quarantine_path", StringType()),
    StructField("message", StringType()),
    StructField("spark_application_id", StringType()),
])

def _to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

def write_audit(status, silver_rows_read=None, valid_rows=None, quarantined_rows=None,
                staging_counts=None, source_path=None, target_schema=None,
                quarantine_path=None, message=None):
    """Append one audit row. Never fails the notebook: the data load is already finished."""
    try:
        end_time = datetime.now(timezone.utc)
        sc = staging_counts or {}
        row = (
            end_time,
            (p_run_id or None),
            NOTEBOOK_NAME,
            AUDIT_LAYER,
            _to_int(p_year),
            _to_int(p_month),
            status,
            run_start_time,
            end_time,
            int((end_time - run_start_time).total_seconds()),
            _to_int(silver_rows_read),
            _to_int(valid_rows),
            _to_int(quarantined_rows),
            _to_int(sc.get("Fact_Orders_stg")),
            _to_int(sc.get("Dim_Product_stg")),
            _to_int(sc.get("Dim_Customer_stg")),
            _to_int(sc.get("Dim_Channel_stg")),
            _to_int(sc.get("Dim_Return_Reason_stg")),
            _to_int(sc.get("Dim_Payment_Method_stg")),
            source_path,
            target_schema,
            quarantine_path,
            message,
            spark.sparkContext.applicationId,
        )
        spark.createDataFrame([row], schema=AUDIT_SCHEMA) \
            .write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)
        print(f"Audit row written to {AUDIT_TABLE}: {status}")
    except Exception as e:
        print(f"WARNING: could not write the audit row to {AUDIT_TABLE}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 4. Functions

# MARKDOWN ********************

# #### 4.1 Parameter validation and reading

# CELL ********************

def validate_parameters():
    """Fail fast when the pipeline passes an invalid year or month."""
    if not re.fullmatch(r"\d{4}", str(p_year).strip()):
        raise ValueError(f"p_year must be a 4 digit year, got '{p_year}'")

    if not re.fullmatch(r"\d{1,2}", str(p_month).strip()) or not (1 <= int(p_month) <= 12):
        raise ValueError(f"p_month must be between 1 and 12, got '{p_month}'")


def read_silver_month():
    """Read the Silver table and return (all_silver_df, rows_for_the_requested_month).
    A missing table or missing column is a pipeline error, not bad data."""
    if not DeltaTable.isDeltaTable(spark, sourceFilePath):
        raise Exception(f"Silver Delta table not found at {sourceFilePath}")

    silver_df = spark.read.format("delta").load(sourceFilePath)

    missing_cols = [c for c in REQUIRED_COLS if c not in silver_df.columns]
    if missing_cols:
        raise Exception(f"Silver table is missing required columns: {missing_cols}")

    month_df = silver_df.filter(
        (col("Order_Year") == int(p_year)) &
        (col("Order_Month") == int(p_month))
    )
    return silver_df, month_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 4.2 Row-level validation
# 
# Staging is overwritten on every run, so an empty month still flows through and clears stale staging data.

# CELL ********************

def is_blank(c):
    return col(c).isNull() | (trim(col(c).cast("string")) == "")


def build_checks():
    """Return the list of rule expressions, in the order their messages should appear."""
    checks = []

    def check(condition, message):
        checks.append(when(condition, lit(message)))

    # Free-text columns look like real names, not stray symbols or numbers-only noise
    for c in TEXT_PATTERN_COLS:
        check(~is_blank(c) & ~trim(col(c)).rlike(TEXT_PATTERN), f"Invalid text: {c}")

    # Subcategory must belong to the Category
    for cat, subs in SUBCATEGORY_BY_CATEGORY.items():
        check(
            (lower(trim(col("Category"))) == cat.lower()) & ~is_blank("Subcategory")
            & ~lower(trim(col("Subcategory"))).isin([s.lower() for s in subs]),
            "Subcategory does not belong to Category"
        )

    for c in FACT_KEY_COLS:
        check(is_blank(c), f"Missing mandatory value: {c}")

    # Numeric ranges and date order
    check(col("Quantity").isNull() | (col("Quantity") <= 0), "Quantity must be greater than 0")
    check(col("Unit_Price").isNull() | (col("Unit_Price") < 0), "Unit_Price missing or negative")
    check(col("Return_Quantity") < 0, "Return_Quantity cannot be negative")
    check(col("Return_Quantity") > col("Quantity"), "Return_Quantity exceeds Quantity")
    check(col("Unit_Cost") < 0, "Unit_Cost cannot be negative")
    check(col("Shipping_Fee") < 0, "Shipping_Fee cannot be negative")
    check((col("Discount_Pct") < 0) | (col("Discount_Pct") > 100), "Discount_Pct outside 0-100")
    check((col("Refund_Deduction_Pct") < 0) | (col("Refund_Deduction_Pct") > 100), "Refund_Deduction_Pct outside 0-100")
    check((col("Product_Rating") < 1) | (col("Product_Rating") > 5), "Product_Rating outside 1-5")
    check(col("Delivery_Date") < col("Order_Date"), "Delivery_Date before Order_Date")
    check(col("Return_Date") < col("Order_Date"), "Return_Date before Order_Date")

    # Return details must agree with Return_Flag
    check(~col("Return_Flag").isin("Yes", "No") | col("Return_Flag").isNull(), "Return_Flag must be Yes or No")
    check((col("Return_Flag") == "Yes") & is_blank("Return_Reason"), "Return_Flag is Yes but Return_Reason is missing")
    check((col("Return_Flag") == "Yes") & col("Return_Date").isNull(), "Return_Flag is Yes but Return_Date is missing")

    return checks


def add_validation_errors(df, checks):
    """Add `Validation_Errors`: all failed-rule messages joined by '; ', or NULL when the row is clean."""
    errors = concat_ws("; ", *checks)
    return df.withColumn(
        "Validation_Errors",
        when(errors == "", lit(None).cast("string")).otherwise(errors)
    )


def quarantine_duplicate_keys(df):
    """Duplicate Order_Line_ID: keep one row (clean first, latest return date), quarantine the rest,
    so the fact table stays unique."""
    dedupe_window = Window.partitionBy("Order_Line_ID").orderBy(
        when(col("Validation_Errors").isNull(), 0).otherwise(1),
        col("Return_Date").desc_nulls_last()
    )
    return (df
        .withColumn("_rn", row_number().over(dedupe_window))
        .withColumn(
            "Validation_Errors",
            when(
                (col("_rn") > 1) & ~is_blank("Order_Line_ID"),
                concat_ws("; ", col("Validation_Errors"), lit("Duplicate Order_Line_ID in silver"))
            ).otherwise(col("Validation_Errors")))
        .drop("_rn")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 4.3 Split, counts and quarantine

# CELL ********************

def split_valid_and_quarantine(df, silver_columns):
    valid_df = df.filter(col("Validation_Errors").isNull()).select(*silver_columns)

    quarantine_df = df.filter(col("Validation_Errors").isNotNull()).select(
        *silver_columns,
        col("Validation_Errors"),
        current_timestamp().alias("Quarantined_At")
    )
    return valid_df, quarantine_df


def count_rows(df):
    """One pass gives the totals AND the expected row count of every staging table
    (each dimension has one row per distinct key; countDistinct ignores NULLs, matching dropna on Return_Reason).
    Returns (input_count, valid_count, quarantine_count, expected_counts)."""
    ok = col("Validation_Errors").isNull()
    c = df.agg(
        count(lit(1)).alias("total"),
        sum(when(ok, 1).otherwise(0)).alias("valid"),
        countDistinct(when(ok, col("Product_ID"))).alias("products"),
        countDistinct(when(ok, col("Customer_ID"))).alias("customers"),
        countDistinct(when(ok, col("Sales_Channel"))).alias("channels"),
        countDistinct(when(ok, col("Return_Reason"))).alias("return_reasons"),
        countDistinct(when(ok, col("Payment_Method"))).alias("payment_methods"),
    ).first()

    input_count = c["total"]
    valid_count = c["valid"] or 0
    expected_counts = {
        "Fact_Orders_stg": valid_count,
        "Dim_Product_stg": c["products"],
        "Dim_Customer_stg": c["customers"],
        "Dim_Channel_stg": c["channels"],
        "Dim_Return_Reason_stg": c["return_reasons"],
        "Dim_Payment_Method_stg": c["payment_methods"],
    }
    return input_count, valid_count, input_count - valid_count, expected_counts


def write_quarantine(quarantine_df):
    """Overwrite, so a re-run of the same month does not duplicate quarantined rows."""
    quarantine_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(quarantinePath)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 4.4 Fact and dimension tables

# CELL ********************

def build_fact(valid_df):
    return valid_df.drop(
        "Customer_Segment", "Customer_City", "Customer_State",
        "Product_Name", "Category", "Subcategory", "Brand"
    )


def build_dimensions(valid_df):
    """Return {staging table name: DataFrame}, one row per distinct key."""
    return {
        "Dim_Product_stg": valid_df.select(
            "Product_ID", "Product_Name", "Category", "Subcategory", "Brand"
        ).drop_duplicates(["Product_ID"]),

        "Dim_Customer_stg": valid_df.select(
            "Customer_ID", "Customer_Segment", "Customer_City", "Customer_State"
        ).drop_duplicates(["Customer_ID"]),

        "Dim_Channel_stg": valid_df.select(
            "Sales_Channel"
        ).drop_duplicates(["Sales_Channel"]),

        "Dim_Return_Reason_stg": valid_df.select(
            "Return_Reason"
        ).drop_duplicates(["Return_Reason"]).dropna(subset=["Return_Reason"]),

        "Dim_Payment_Method_stg": valid_df.select(
            "Payment_Method"
        ).drop_duplicates(["Payment_Method"]),
    }


def report_conflicts(frame, key, attrs, name):
    """Warning only: drop_duplicates picks an arbitrary row per ID, so flag IDs with conflicting attributes."""
    conflicts = frame.select(key, *attrs).distinct() \
                     .groupBy(key).count().filter(col("count") > 1).count()
    if conflicts > 0:
        print(f"WARNING: {conflicts} {key} value(s) have conflicting {name} attributes in this batch")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 4.5 Staging write

# CELL ********************

def write_to_staging(df, table_name, expected_counts):
    """Overwrite a staging table, then read it back and compare with the expected row count."""
    target_table = f"{Target_Warehouse}.{Target_Schema}.{table_name}"
    row_count = expected_counts[table_name]

    df.write \
        .mode("overwrite") \
        .synapsesql(target_table)

    loaded_count = spark.read.synapsesql(target_table).count()
    if loaded_count != row_count:
        raise Exception(
            f"Staging load mismatch for {target_table}: expected {row_count}, found {loaded_count}"
        )

    print(f"Successfully loaded: {target_table}")
    print(f"Rows loaded: {row_count}")
    return row_count

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 5. Run

# MARKDOWN ********************

# #### 5.1 Read and validate

# CELL ********************

validate_parameters()

silver_df, df = read_silver_month()
df.printSchema()

df = add_validation_errors(df, build_checks())
df = quarantine_duplicate_keys(df)
df.persist(StorageLevel.MEMORY_AND_DISK)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 5.2 Split, count and quarantine

# CELL ********************

valid_df, quarantine_df = split_valid_and_quarantine(df, silver_df.columns)
input_count, valid_count, quarantine_count, expected_counts = count_rows(df)

write_quarantine(quarantine_df)

print(f"Silver rows for {p_year}-{p_month}: {input_count}")
print(f"Valid rows: {valid_count}")
print(f"Quarantined rows: {quarantine_count} -> {quarantinePath}")

if input_count == 0:
    print("WARNING: no silver rows for this month, staging tables will be emptied.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 5.3 Build fact and dimensions

# CELL ********************

fact_orders_df = build_fact(valid_df)
dimensions = build_dimensions(valid_df)

report_conflicts(valid_df, "Product_ID",
                 ["Product_Name", "Category", "Subcategory", "Brand"], "product")
report_conflicts(valid_df, "Customer_ID",
                 ["Customer_Segment", "Customer_City", "Customer_State"], "customer")

fact_orders_df.printSchema()
for dim_df in dimensions.values():
    dim_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 5.4 Write to staging

# CELL ********************

fact_row_count = write_to_staging(fact_orders_df, "Fact_Orders_stg", expected_counts)
for table_name, dim_df in dimensions.items():
    write_to_staging(dim_df, table_name, expected_counts)

print(f"Quarantined rows: {quarantine_count}")
df.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 6. Audit and exit

# CELL ********************

write_audit(
    status="Success" if input_count > 0 else "No Data",
    silver_rows_read=input_count,
    valid_rows=valid_count,
    quarantined_rows=quarantine_count,
    staging_counts=expected_counts,
    source_path=sourceFilePath,
    target_schema=f"{Target_Warehouse}.{Target_Schema}",
    quarantine_path=quarantinePath,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(str(fact_row_count))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
