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
p_max_reject_pct="5"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Bronze → Silver
# 
# Cleans, validates and loads one month of ecommerce returns data from the Bronze layer into the Silver Delta table.
# 
# | | |
# |---|---|
# | **Input** | `Files/bronze/ecommerce_returns/year=<p_year>/month=<p_month>/` (parquet) |
# | **Output** | `Files/silver/ecommerce_returns/` (Delta, merged on `Order_Line_ID`) |
# | **Quarantine** | `Files/quarantine/ecommerce_returns/bronze_silver/year=<p_year>month=<p_month>/` (Delta, rejected rows + reasons) |
# | **Audit** | one row appended to the Lakehouse table `notebook_audit` |
# | **Exit value** | number of rows loaded into Silver (`"0"` when there is no bronze data) |
# 
# **Parameters** (first cell): `p_year`, `p_month`, `p_run_id` (`@pipeline().RunId` to link audit rows to the pipeline run).
# 
# **Flow:** validate parameters → read bronze → align schema → keep raw copy → clean → validate rows → de-duplicate keys → split valid / quarantine → merge into Silver → audit → exit.

# MARKDOWN ********************

# ### 1. Setup

# CELL ********************

import re
from functools import reduce
from datetime import datetime, timezone

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from pyspark import StorageLevel
from delta.tables import DeltaTable

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Configuration

# MARKDOWN ********************

# #### 2.1 Paths

# CELL ********************

sourceFilePath = f"Files/bronze/ecommerce_returns/year={p_year}/month={p_month}/"
targetFilePath = "Files/silver/ecommerce_returns/"
quarantinePath = f"Files/quarantine/ecommerce_returns/bronze_silver/year={p_year}/month={p_month}/"

KEY_COL = "Order_Line_ID"   # business key used for de-duplication and the Silver merge

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 2.2 Column definitions

# CELL ********************

# Columns the pipeline expects in the source (missing ones are added as NULL)
REFERENCED_COLS = [
    "Order_Line_ID", "Order_ID", "Order_Date", "Customer_ID", "Customer_Segment",
    "Customer_City", "Customer_State", "Product_ID", "Product_Name", "Category",
    "Subcategory", "Brand", "Sales_Channel", "Payment_Method", "Quantity",
    "Unit_Price", "Unit_Cost", "Discount_Pct", "Shipping_Fee", "Delivery_Date",
    "Return_Flag", "Return_Date", "Return_Reason", "Return_Quantity",
    "Refund_Deduction_Pct", "Product_Rating",
]

# Type conversions applied during cleaning
DATE_COLS = ["Order_Date", "Delivery_Date", "Return_Date"]
INT_COLS = ["Quantity", "Return_Quantity", "Product_Rating"]
DECIMAL_COLS = {
    "Unit_Price": "decimal(12,2)",
    "Unit_Cost": "decimal(12,2)",
    "Discount_Pct": "decimal(6,2)",
    "Shipping_Fee": "decimal(12,2)",
    "Refund_Deduction_Pct": "decimal(6,2)",
}
TEXT_COLS = ["Sales_Channel", "Payment_Method", "Return_Reason", "Return_Flag"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 2.3 Validation rules

# CELL ********************

# Values that must be present (blank = row quarantined)
MANDATORY_COLS = [
    "Order_Line_ID", "Order_ID", "Order_Date", "Customer_ID", "Product_ID",
    "Quantity", "Unit_Price", "Sales_Channel", "Payment_Method",
]

# ID formats
FORMAT_RULES = {
    "Order_Line_ID": r"^OL-\d{6,}$",
    "Order_ID": r"^ORD-\d{4}-\d{5,}$",
    "Customer_ID": r"^C\d{4,}$",
    "Product_ID": r"^P\d{3,}$",
}

# Allowed category values (compared case-insensitively, after cleaning)
ALLOWED_VALUES = {
    "Customer_Segment": ["Consumer", "Corporate", "Home Office"],
    "Category": ["Fashion", "Electronics", "Home & Kitchen", "Beauty", "Sports", "Books"],
    "Sales_Channel": ["Website", "Mobile App", "Marketplace", "Offline Store"],
    "Payment_Method": ["Upi", "Credit Card", "Debit Card", "Cash On Delivery", "Wallet", "Net Banking", "Cash"],
    "Return_Reason": ["Wrong Size", "Damaged Product", "Product Not As Expected", "Quality Issue",
                      "Wrong Item", "Defective Product", "Late Delivery", "Changed Mind"],
}

# Valid Subcategory for each Category
SUBCATEGORY_BY_CATEGORY = {
    "Beauty": ["Personal Care", "Skin Care"],
    "Books": ["Books"],
    "Electronics": ["Mobile Accessories", "Smart Devices", "Audio"],
    "Fashion": ["Men's Clothing", "Women's Clothing", "Footwear"],
    "Home & Kitchen": ["Home Furnishing", "Kitchen"],
    "Sports": ["Fitness", "Outdoor"],
}

# Free-text columns must look like real names (2-100 chars)
TEXT_PATTERN_COLS = ["Customer_City", "Customer_State", "Product_Name", "Subcategory", "Brand"]
TEXT_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9 &'.,()/+\-]{1,99}$"

# Accepted spellings of Return_Flag in the source (cleaning maps these to Yes / No)
RETURN_FLAG_VALUES = ["yes", "y", "returned", "no", "n", "not returned"]

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
# The row is written at the end of the notebook; a failed run raises before this point
# and is recorded by the pipeline in Returns_Control.Pipeline_Metadata instead.
AUDIT_TABLE = "audit_bronze_silver"
NOTEBOOK_NAME = "Bronze_Silver_Notebook_Optimised"
AUDIT_LAYER = "bronze_to_silver"
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
    StructField("input_rows", LongType()),
    StructField("valid_rows", LongType()),
    StructField("quarantined_rows", LongType()),
    StructField("rows_written", LongType()),
    StructField("rows_inserted", LongType()),
    StructField("rows_updated", LongType()),
    StructField("source_path", StringType()),
    StructField("target_path", StringType()),
    StructField("quarantine_path", StringType()),
    StructField("schema_drift_columns", StringType()),
    StructField("missing_source_columns", StringType()),
    StructField("message", StringType()),
    StructField("spark_application_id", StringType()),
])

def _to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

def write_audit(status, input_rows=None, valid_rows=None, quarantined_rows=None,
                rows_written=None, rows_inserted=None, rows_updated=None,
                source_path=None, target_path=None, quarantine_path=None,
                schema_drift_columns=None, missing_source_columns=None, message=None):
    """Append one audit row. Never fails the notebook: the data load is already finished."""
    try:
        end_time = datetime.now(timezone.utc)
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
            _to_int(input_rows),
            _to_int(valid_rows),
            _to_int(quarantined_rows),
            _to_int(rows_written),
            _to_int(rows_inserted),
            _to_int(rows_updated),
            source_path,
            target_path,
            quarantine_path,
            schema_drift_columns,
            missing_source_columns,
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
# 
# Each function below does one step of the flow and returns the new DataFrame (plus any extra information the later steps need).

# MARKDOWN ********************

# #### 4.1 Parameter validation and reading

# CELL ********************

def validate_parameters():
    """Fail fast when the pipeline passes an invalid year or month."""
    if not re.fullmatch(r"\d{4}", str(p_year).strip()):
        raise ValueError(f"p_year must be a 4 digit year, got '{p_year}'")

    if not re.fullmatch(r"\d{1,2}", str(p_month).strip()) or not (1 <= int(p_month) <= 12):
        raise ValueError(f"p_month must be between 1 and 12, got '{p_month}'")

    try:
        if not (0 <= float(p_max_reject_pct) <= 100):
            raise ValueError
    except ValueError:
        raise ValueError(f"p_max_reject_pct must be a number between 0 and 100, got '{p_max_reject_pct}'")


def read_bronze():
    """Read the month's bronze parquet. Raises if the folder is unreadable."""
    try:
        return spark.read.parquet(sourceFilePath)
    except AnalysisException as e:
        raise Exception(f"Bronze source not found or unreadable: {sourceFilePath}") from e


def exit_if_empty(df):
    """Log a 'No Data' audit row and stop the notebook cleanly when bronze is empty."""
    if len(df.columns) == 0 or len(df.head(1)) == 0:
        print(f"No bronze data found at {sourceFilePath}. Nothing to load.")
        write_audit(
            status="No Data", input_rows=0, valid_rows=0, quarantined_rows=0,
            rows_written=0, rows_inserted=0, rows_updated=0,
            source_path=sourceFilePath, target_path=targetFilePath, quarantine_path=quarantinePath,
            message="No bronze data found; nothing loaded",
        )
        mssparkutils.notebook.exit("0")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 4.2 Schema alignment

# CELL ********************

def add_missing_columns(df):
    """Add expected columns that are absent from the source as NULL strings.
    Returns (df, missing_cols)."""
    missing_cols = [c for c in REFERENCED_COLS if c not in df.columns]
    for c in missing_cols:
        df = df.withColumn(c, lit(None).cast("string"))
    if missing_cols:
        print(f"WARNING: columns missing from source, filled with NULL: {missing_cols}")
    return df, missing_cols


def align_with_silver(df):
    """Compare the source with the existing Silver table (if any).
    - Silver columns absent from the source are added as NULL of the Silver type, so the merge lines up.
    - Source columns unknown to Silver are reported as schema drift.
    Returns (df, extra_cols)."""
    extra_cols = []
    if DeltaTable.isDeltaTable(spark, targetFilePath):
        target_schema = spark.read.format("delta").load(targetFilePath).schema
        target_names = [f.name for f in target_schema.fields]

        for f in target_schema.fields:
            if f.name in ("Order_Year", "Order_Month"):
                continue
            if f.name not in df.columns:
                df = df.withColumn(f.name, lit(None).cast(f.dataType))
                print(f"WARNING: silver column '{f.name}' missing from source, filled with NULL")

        extra_cols = [c for c in df.columns
                      if c not in target_names and c not in REFERENCED_COLS]
        if extra_cols:
            print(f"WARNING: schema drift, new columns in source: {extra_cols}")
    return df, extra_cols


def add_raw_copies(df):
    """Keep an untouched string copy of every column (`_raw_<col>`).
    The raw values are used by the checks and logged for quarantined rows.
    Returns (df, original_cols)."""
    original_cols = df.columns
    df = df.select(
        "*",
        *[col(c).cast("string").alias(f"_raw_{c}") for c in original_cols]
    )
    return df, original_cols

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 4.3 Cleaning

# CELL ********************

def clean_data(df):
    """Convert types and standardise text values."""
    # Decimals: strip currency symbols / separators before casting
    for c in DECIMAL_COLS:
        df = df.withColumn(c, regexp_replace(col(c).cast("string"), r"[^0-9.-]", ""))

    for c in DATE_COLS:
        df = df.withColumn(c, to_date(trim(col(c).cast("string")), "dd-MMM-yyyy"))

    for c in INT_COLS:
        df = df.withColumn(c, trim(col(c).cast("string")).cast("int"))

    for c, dtype in DECIMAL_COLS.items():
        df = df.withColumn(c, col(c).cast(dtype))

    # Text standardisation
    df = (df
        .withColumn("Sales_Channel", initcap(trim(col("Sales_Channel"))))
        .withColumn("Payment_Method", initcap(trim(col("Payment_Method"))))
        .withColumn("Return_Reason", initcap(trim(col("Return_Reason"))))
        .withColumn("Payment_Method", regexp_replace(col("Payment_Method"), "Cod", "Cash On Delivery"))
    )

    # Return_Flag -> Yes / No, and one known Return_Reason alias
    df = (df
        .withColumn(
            "Return_Flag",
            when(lower(trim(col("Return_Flag"))).isin("yes", "y", "returned"), "Yes").otherwise("No"))
        .withColumn(
            "Return_Reason",
            when(col("Return_Reason") == "Not As Expected", "Product Not As Expected")
            .otherwise(col("Return_Reason")))
    )

    # Partition columns
    return (df
        .withColumn("Order_Year", year(col("Order_Date")))
        .withColumn("Order_Month", month(col("Order_Date")))
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 4.4 Row-level validation
# 
# `build_checks` returns a list of conditions; each one yields a message when the rule fails.
# All messages for a row are joined into `Validation_Errors`, and rows with any message are quarantined.

# CELL ********************

def is_blank(raw_col):
    return col(raw_col).isNull() | (trim(col(raw_col)) == "")


def build_checks(extra_cols):
    """Return the list of rule expressions, in the order their messages should appear."""
    checks = []

    def check(condition, message):
        checks.append(when(condition, lit(message)))

    # --- Completeness and format -------------------------------------------------
    for c in MANDATORY_COLS:
        check(is_blank(f"_raw_{c}"), f"Missing mandatory value: {c}")

    # A value was supplied but could not be converted to its type
    for c in DATE_COLS + INT_COLS + list(DECIMAL_COLS):
        check(~is_blank(f"_raw_{c}") & col(c).isNull(), f"Invalid value: {c}")

    for c, pattern in FORMAT_RULES.items():
        check(~is_blank(f"_raw_{c}") & ~trim(col(f"_raw_{c}")).rlike(pattern), f"Invalid format: {c}")

    # --- Reference values --------------------------------------------------------
    # (blank values are caught by the mandatory check, or are optional)
    for c, vals in ALLOWED_VALUES.items():
        allowed = [v.lower() for v in vals]
        check(~is_blank(f"_raw_{c}") & ~lower(trim(col(c))).isin(allowed), f"Unexpected value: {c}")

    for c in TEXT_PATTERN_COLS:
        check(~is_blank(f"_raw_{c}") & ~trim(col(f"_raw_{c}")).rlike(TEXT_PATTERN), f"Invalid text: {c}")

    for cat, subs in SUBCATEGORY_BY_CATEGORY.items():
        check(
            (lower(trim(col("Category"))) == cat.lower()) & ~is_blank("_raw_Subcategory")
            & ~lower(trim(col("Subcategory"))).isin([s.lower() for s in subs]),
            "Subcategory does not belong to Category"
        )

    # The cleaning step turns any unknown Return_Flag into "No", so check the raw spelling
    check(~is_blank("_raw_Return_Flag") & ~lower(trim(col("_raw_Return_Flag"))).isin(RETURN_FLAG_VALUES),
          "Unexpected value: Return_Flag")

    # --- Dates -------------------------------------------------------------------
    check(col("Order_Date") > current_date(), "Order_Date is in the future")
    check(col("Return_Date") > current_date(), "Return_Date is in the future")

    # --- Return details must agree with Return_Flag -------------------------------
    check((col("Return_Flag") == "Yes") & is_blank("_raw_Return_Date"), "Return_Flag is Yes but Return_Date is missing")
    check((col("Return_Flag") == "Yes") & is_blank("_raw_Return_Reason"), "Return_Flag is Yes but Return_Reason is missing")
    check((col("Return_Flag") == "No") & ~is_blank("_raw_Return_Date"), "Return_Flag is No but Return_Date is present")
    check((col("Return_Flag") == "No") & ~is_blank("_raw_Return_Reason"), "Return_Flag is No but Return_Reason is present")
    check(col("Return_Date") < col("Delivery_Date"), "Return_Date before Delivery_Date")

    # --- Row belongs to the requested month ---------------------------------------
    check(
        (col("Order_Year") != int(p_year)) | (col("Order_Month") != int(p_month)),
        "Order_Date outside the requested year/month"
    )

    # --- Business rules (a NULL comparison is not a failure; missing/invalid values are caught above)
    check(col("Quantity") <= 0, "Quantity must be greater than 0")
    check(col("Return_Quantity") < 0, "Return_Quantity cannot be negative")
    check(col("Return_Quantity") > col("Quantity"), "Return_Quantity exceeds Quantity")
    check(col("Unit_Price") < 0, "Unit_Price cannot be negative")
    check(col("Unit_Cost") < 0, "Unit_Cost cannot be negative")
    check(col("Shipping_Fee") < 0, "Shipping_Fee cannot be negative")
    check((col("Discount_Pct") < 0) | (col("Discount_Pct") > 100), "Discount_Pct outside 0-100")
    check((col("Refund_Deduction_Pct") < 0) | (col("Refund_Deduction_Pct") > 100), "Refund_Deduction_Pct outside 0-100")
    check((col("Product_Rating") < 1) | (col("Product_Rating") > 5), "Product_Rating outside 1-5")
    check(col("Delivery_Date") < col("Order_Date"), "Delivery_Date before Order_Date")
    check(col("Return_Date") < col("Order_Date"), "Return_Date before Order_Date")
    check(
        (col("Return_Flag") == "Yes") & (col("Return_Quantity").isNull() | (col("Return_Quantity") <= 0)),
        "Return_Flag is Yes but Return_Quantity is missing or zero"
    )
    check(
        (col("Return_Flag") == "No") & (col("Return_Quantity") > 0),
        "Return_Flag is No but Return_Quantity is greater than 0"
    )

    # --- Schema drift: a value in a column that is not in the Silver schema --------
    if extra_cols:
        drift_condition = reduce(lambda a, b: a | b, [~is_blank(f"_raw_{c}") for c in extra_cols])
        check(drift_condition, f"Schema drift: value in unexpected column(s) {extra_cols}")

    return checks

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def add_validation_errors(df, checks):
    """Add `Validation_Errors`: all failed-rule messages joined by '; ', or NULL when the row is clean."""
    errors = concat_ws("; ", *checks)
    return df.withColumn(
        "Validation_Errors",
        when(errors == "", lit(None).cast("string")).otherwise(errors)
    )


def quarantine_duplicate_keys(df):
    """Duplicate keys within the batch: keep the best row (clean first, latest return/delivery date)
    and quarantine the rest. Without this the Delta MERGE would fail with several source rows
    matching one target row."""
    dedupe_window = Window.partitionBy(KEY_COL).orderBy(
        when(col("Validation_Errors").isNull(), 0).otherwise(1),
        col("Return_Date").desc_nulls_last(),
        col("Delivery_Date").desc_nulls_last()
    )
    return (df
        .withColumn("_rn", row_number().over(dedupe_window))
        .withColumn(
            "Validation_Errors",
            when(
                (col("_rn") > 1) & ~is_blank(f"_raw_{KEY_COL}"),
                concat_ws("; ", col("Validation_Errors"), lit(f"Duplicate {KEY_COL} in batch"))
            ).otherwise(col("Validation_Errors")))
        .drop("_rn")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 4.5 Split, quarantine and Silver load

# CELL ********************

def split_valid_and_quarantine(df, original_cols, extra_cols):
    """Valid rows keep the cleaned Silver columns; quarantined rows keep the original raw values plus the reasons."""
    valid_cols = [c for c in original_cols if c not in extra_cols] + ["Order_Year", "Order_Month"]

    valid_df = df.filter(col("Validation_Errors").isNull()).select(*valid_cols)

    quarantine_df = df.filter(col("Validation_Errors").isNotNull()).select(
        *[col(f"_raw_{c}").alias(c) for c in original_cols],
        col("Validation_Errors"),
        current_timestamp().alias("Quarantined_At"),
        lit(str(p_year)).alias("Source_Year"),
        lit(str(p_month)).alias("Source_Month")
    )
    return valid_df, quarantine_df


def count_rows(df):
    """One pass over the cached data gives every count (instead of one Spark job per count)."""
    counts = df.agg(
        count(lit(1)).alias("total"),
        sum(when(col("Validation_Errors").isNull(), 1).otherwise(0)).alias("valid")
    ).first()
    total = counts["total"]
    valid = counts["valid"] or 0
    return total, valid, total - valid


def enforce_reject_threshold(input_count, valid_count, quarantine_count):
    """Stop before touching Silver when too many rows were rejected (usually a broken source file).
    The quarantine table is already written, so the rejected rows can be inspected.
    Raising makes the pipeline take its failure path: failure email, watermark not advanced."""
    reject_pct = (quarantine_count / input_count * 100) if input_count else 0.0
    print(f"Reject rate: {reject_pct:.1f}% (limit {float(p_max_reject_pct):g}%)")

    if reject_pct > float(p_max_reject_pct):
        message = (f"Reject rate {reject_pct:.1f}% exceeds the {float(p_max_reject_pct):g}% limit "
                   f"({quarantine_count} of {input_count} rows quarantined)")
        write_audit(
            status="Rejected", input_rows=input_count, valid_rows=valid_count,
            quarantined_rows=quarantine_count, rows_written=0, rows_inserted=0, rows_updated=0,
            source_path=sourceFilePath, target_path=targetFilePath, quarantine_path=quarantinePath,
            message=message,
        )
        raise Exception(f"{message}. Silver not loaded. Check {quarantinePath}")


def write_quarantine(quarantine_df, quarantine_count):
    """Overwrite, so a re-run of the same month does not duplicate quarantined rows."""
    quarantine_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(quarantinePath)

    if quarantine_count > 0:
        # Summarise from the quarantine table just written (small), not from the full dataset
        spark.read.format("delta").load(quarantinePath) \
            .select(explode(split(col("Validation_Errors"), "; ")).alias("Rule")) \
            .withColumn("Rule", regexp_replace(col("Rule"), r"\[.*\]", "[...]")) \
            .groupBy("Rule").count().orderBy(col("count").desc()).show(50, truncate=False)


def load_silver(valid_df, valid_count):
    """Merge valid rows into Silver on the key (first load creates the partitioned table).
    Returns the Delta merge metrics ({} on first load or when there is nothing to load)."""
    merge_metrics = {}

    if valid_count == 0:
        print("No valid rows to load into silver.")
        return merge_metrics

    if DeltaTable.isDeltaTable(spark, targetFilePath):
        silver_table = DeltaTable.forPath(spark, targetFilePath)

        silver_table.alias("target") \
            .merge(valid_df.alias("source"), f"target.{KEY_COL} = source.{KEY_COL}") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

        # Metrics come from the Delta log, no extra scan needed
        merge_metrics = silver_table.history(1).select("operationMetrics").first()[0] or {}
    else:
        valid_df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("Order_Year", "Order_Month") \
            .save(targetFilePath)

    return merge_metrics

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 5. Run

# MARKDOWN ********************

# #### 5.1 Read and prepare

# CELL ********************

validate_parameters()

df = read_bronze()
exit_if_empty(df)
df.printSchema()

df, missing_cols = add_missing_columns(df)
df, extra_cols = align_with_silver(df)

df = df.drop_duplicates()                 # exact duplicate rows (counts are taken once, after validation)
df, original_cols = add_raw_copies(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 5.2 Clean and validate

# CELL ********************

df = clean_data(df)
df.printSchema()

df = add_validation_errors(df, build_checks(extra_cols))
df = quarantine_duplicate_keys(df)

df = df.persist(StorageLevel.MEMORY_AND_DISK)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 5.3 Quarantine and load Silver

# CELL ********************

valid_df, quarantine_df = split_valid_and_quarantine(df, original_cols, extra_cols)
input_count, valid_count, quarantine_count = count_rows(df)

write_quarantine(quarantine_df, quarantine_count)

print(f"Input rows (after exact de-dup): {input_count}")
print(f"Valid rows: {valid_count}")
print(f"Quarantined rows: {quarantine_count} -> {quarantinePath}")

enforce_reject_threshold(input_count, valid_count, quarantine_count)

merge_metrics = load_silver(valid_df, valid_count)
silver_record_processed_count = valid_count

print(f"Rows Processed in silver: {silver_record_processed_count}")
print(f"Rows Quarantined: {quarantine_count}")
if merge_metrics:
    print(f"Silver rows inserted: {merge_metrics.get('numTargetRowsInserted')}, "
          f"updated: {merge_metrics.get('numTargetRowsUpdated')}")

df.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 6. Audit and exit

# CELL ********************

if merge_metrics:
    rows_inserted = _to_int(merge_metrics.get("numTargetRowsInserted"))
    rows_updated = _to_int(merge_metrics.get("numTargetRowsUpdated"))
else:
    # First load (table just created) or nothing to load
    rows_inserted = valid_count
    rows_updated = 0

write_audit(
    status="Success",
    input_rows=input_count,
    valid_rows=valid_count,
    quarantined_rows=quarantine_count,
    rows_written=silver_record_processed_count,
    rows_inserted=rows_inserted,
    rows_updated=rows_updated,
    source_path=sourceFilePath,
    target_path=targetFilePath,
    quarantine_path=quarantinePath,
    schema_drift_columns=", ".join(extra_cols) if extra_cols else None,
    missing_source_columns=", ".join(missing_cols) if missing_cols else None,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(str(silver_record_processed_count))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
