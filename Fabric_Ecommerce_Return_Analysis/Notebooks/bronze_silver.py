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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sourceFilePath = f"Files/bronze/ecommerce_returns/year={p_year}/month={p_month}/"
targetFilePath = "Files/silver/ecommerce_returns/"
quarantinePath = f"Files/quarantine/ecommerce_returns/bronze_silver/year={p_year}/month={p_month}/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re
from functools import reduce
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
from pyspark import StorageLevel

# Bad values become NULL (and get quarantined) instead of throwing
spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------- Validation configuration ----------------
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

# A blank value in any of these sends the row to quarantine
MANDATORY_COLS = [
    "Order_Line_ID", "Order_Date", "Product_ID", "Customer_ID",
    "Quantity", "Unit_Price", "Sales_Channel", "Payment_Method",
]

KEY_COL = "Order_Line_ID"

# Columns the code below depends on; added as NULL if absent from the source file
REFERENCED_COLS = list(dict.fromkeys(
    MANDATORY_COLS + DATE_COLS + INT_COLS + list(DECIMAL_COLS) + TEXT_COLS
))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------- Validation 1: notebook parameters ----------------
if not re.fullmatch(r"\d{4}", str(p_year).strip()):
    raise ValueError(f"p_year must be a 4 digit year, got '{p_year}'")

if not re.fullmatch(r"\d{1,2}", str(p_month).strip()) or not (1 <= int(p_month) <= 12):
    raise ValueError(f"p_month must be between 1 and 12, got '{p_month}'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------- Validation 2: source exists and has data ----------------
try:
    df = spark.read.parquet(sourceFilePath)
except AnalysisException as e:
    raise Exception(f"Bronze source not found or unreadable: {sourceFilePath}") from e

if len(df.columns) == 0 or len(df.head(1)) == 0:
    print(f"No bronze data found at {sourceFilePath}. Nothing to load.")
    mssparkutils.notebook.exit("0")

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------- Validation 3: schema (missing / drifted columns) ----------------
# Columns the code needs but the source file does not have -> added as NULL,
# so the mandatory check quarantines the rows instead of the notebook failing.
missing_cols = [c for c in REFERENCED_COLS if c not in df.columns]
for c in missing_cols:
    df = df.withColumn(c, lit(None).cast("string"))
if missing_cols:
    print(f"WARNING: columns missing from source, filled with NULL: {missing_cols}")

# Schema drift is judged against the existing silver table (if it exists)
extra_cols = []
if DeltaTable.isDeltaTable(spark, targetFilePath):
    target_schema = spark.read.format("delta").load(targetFilePath).schema
    target_names = [f.name for f in target_schema.fields]

    # Silver columns absent from the source -> NULL of the silver type, so the merge schema lines up
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------- Validation 4: exact duplicate rows (counts are taken once, after validation) ----------------
df = df.drop_duplicates()

# Keep an untouched string copy of every column, used to log the original values of quarantined rows
original_cols = df.columns
df = df.select(
    "*",
    *[col(c).cast("string").alias(f"_raw_{c}") for c in original_cols]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------- Cleaning (unchanged logic) ----------------
for c in DECIMAL_COLS:
    df = df.withColumn(c, regexp_replace(col(c).cast("string"), r"[^0-9.-]", ""))

for c in DATE_COLS:
    df = df.withColumn(c, to_date(trim(col(c).cast("string")), "dd-MMM-yyyy"))

for c in INT_COLS:
    df = df.withColumn(c, trim(col(c).cast("string")).cast("int"))

for c, dtype in DECIMAL_COLS.items():
    df = df.withColumn(c, col(c).cast(dtype))

df = df.withColumn(
        "Sales_Channel", initcap(trim(col("Sales_Channel")))
    ).withColumn(
        "Payment_Method", initcap(trim(col("Payment_Method")))
    ).withColumn(
        "Return_Reason", initcap(trim(col("Return_Reason")))
    ).withColumn(
        "Payment_Method",
        regexp_replace(col("Payment_Method"), "Cod", "Cash On Delivery")
    )

df = df.withColumn(
        "Return_Flag",
        when(
            lower(trim(col("Return_Flag"))).isin("yes", "y", "returned"),
            "Yes"
        ).otherwise("No")
    ).withColumn(
        "Return_Reason",
        when(
            col("Return_Reason") == "Not As Expected",
            "Product Not As Expected"
        ).otherwise(col("Return_Reason"))
    )

df = df.withColumn(
        "Order_Year", year(col("Order_Date"))
    ).withColumn(
        "Order_Month", month(col("Order_Date"))
    )

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------- Validation 5: row level checks ----------------
# Every failed rule adds a message to Validation_Errors; rows with any message are quarantined.
checks = []

def check(condition, message):
    checks.append(when(condition, lit(message)))

def is_blank(raw_col):
    return col(raw_col).isNull() | (trim(col(raw_col)) == "")

# Mandatory values present
for c in MANDATORY_COLS:
    check(is_blank(f"_raw_{c}"), f"Missing mandatory value: {c}")

# Value was supplied but could not be converted to its type
for c in DATE_COLS + INT_COLS + list(DECIMAL_COLS):
    check(~is_blank(f"_raw_{c}") & col(c).isNull(), f"Invalid value: {c}")

# Business rules (a NULL comparison is not a failure; missing/invalid values are caught above)
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

# Schema drift: a value present in a column that is not in the silver schema
if extra_cols:
    drift_condition = reduce(lambda a, b: a | b, [~is_blank(f"_raw_{c}") for c in extra_cols])
    check(drift_condition, f"Schema drift: value in unexpected column(s) {extra_cols}")

errors = concat_ws("; ", *checks)
df = df.withColumn(
    "Validation_Errors",
    when(errors == "", lit(None).cast("string")).otherwise(errors)
)

# Duplicate keys within the batch: keep the best row (clean first, latest return/delivery), quarantine the rest.
# Without this the Delta MERGE below would fail with multiple source rows matching one target row.
dedupe_window = Window.partitionBy(KEY_COL).orderBy(
    when(col("Validation_Errors").isNull(), 0).otherwise(1),
    col("Return_Date").desc_nulls_last(),
    col("Delivery_Date").desc_nulls_last()
)
df = df.withColumn("_rn", row_number().over(dedupe_window))
df = df.withColumn(
    "Validation_Errors",
    when(
        (col("_rn") > 1) & ~is_blank(f"_raw_{KEY_COL}"),
        concat_ws("; ", col("Validation_Errors"), lit(f"Duplicate {KEY_COL} in batch"))
    ).otherwise(col("Validation_Errors"))
).drop("_rn")

# Cached once; the single aggregation below materialises it and every later step reuses it
df = df.persist(StorageLevel.MEMORY_AND_DISK)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------- Split valid vs quarantine ----------------
valid_cols = [c for c in original_cols if c not in extra_cols] + ["Order_Year", "Order_Month"]

valid_df = df.filter(col("Validation_Errors").isNull()).select(*valid_cols)

quarantine_df = df.filter(col("Validation_Errors").isNotNull()).select(
    *[col(f"_raw_{c}").alias(c) for c in original_cols],
    col("Validation_Errors"),
    current_timestamp().alias("Quarantined_At"),
    lit(str(p_year)).alias("Source_Year"),
    lit(str(p_month)).alias("Source_Month")
)

# One pass over the cached data gives every count we need (instead of one Spark job per count)
counts = df.agg(
    count(lit(1)).alias("total"),
    sum(when(col("Validation_Errors").isNull(), 1).otherwise(0)).alias("valid")
).first()
input_count = counts["total"]
valid_count = counts["valid"] or 0
quarantine_count = input_count - valid_count

# Overwrite so a re-run of the same month does not duplicate quarantined rows
quarantine_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(quarantinePath)

print(f"Input rows (after exact de-dup): {input_count}")
print(f"Valid rows: {valid_count}")
print(f"Quarantined rows: {quarantine_count} -> {quarantinePath}")

if quarantine_count > 0:
    # Read from the quarantine table just written (small), not from the full dataset
    spark.read.format("delta").load(quarantinePath) \
        .select(explode(split(col("Validation_Errors"), "; ")).alias("Rule")) \
        .withColumn("Rule", regexp_replace(col("Rule"), r"\[.*\]", "[...]")) \
        .groupBy("Rule").count().orderBy(col("count").desc()).show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_record_processed_count = valid_count
merge_metrics = {}

if valid_count > 0:
    if DeltaTable.isDeltaTable(spark, targetFilePath):

        silver_table = DeltaTable.forPath(spark, targetFilePath)

        silver_table.alias("target") \
            .merge(
                valid_df.alias("source"),
                "target.Order_Line_ID = source.Order_Line_ID"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

        # Merge metrics come from the Delta log, no extra scan needed
        merge_metrics = silver_table.history(1).select("operationMetrics").first()[0] or {}

    else:
        valid_df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("Order_Year", "Order_Month") \
            .save(targetFilePath)
else:
    print("No valid rows to load into silver.")

print(f"Rows loaded: {silver_record_processed_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

# CELL ********************

mssparkutils.notebook.exit(str(silver_record_processed_count))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
