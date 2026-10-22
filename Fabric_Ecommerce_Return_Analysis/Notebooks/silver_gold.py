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
# META     },
# META     "warehouse": {
# META       "default_warehouse": "bc3305e3-0c9a-a3b8-4884-b22af9f2dae4",
# META       "known_warehouses": [
# META         {
# META           "id": "bc3305e3-0c9a-a3b8-4884-b22af9f2dae4",
# META           "type": "Datawarehouse"
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

sourceFilePath = "Files/silver/ecommerce_returns/"
quarantinePath = f"Files/quarantine/ecommerce_returns/silver_gold/year={p_year}/month={p_month}/"
Target_Warehouse = "Ecommerce_Returns_Warehouse"
Target_Schema = "Returns_Staging"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
from pyspark import StorageLevel
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

spark.conf.set("spark.sql.ansi.enabled", "false")

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

# ---------------- Validation 2: silver table exists and has the columns we need ----------------
REQUIRED_COLS = [
    "Order_Line_ID", "Order_Date", "Order_Year", "Order_Month",
    "Quantity", "Return_Quantity", "Unit_Price",
    "Product_ID", "Product_Name", "Category", "Subcategory", "Brand",
    "Customer_ID", "Customer_Segment", "Customer_City", "Customer_State",
    "Sales_Channel", "Payment_Method", "Return_Reason",
]
FACT_KEY_COLS = [
    "Order_Line_ID", "Order_Date", "Product_ID", "Customer_ID",
    "Sales_Channel", "Payment_Method",
]

if not DeltaTable.isDeltaTable(spark, sourceFilePath):
    raise Exception(f"Silver Delta table not found at {sourceFilePath}")

silver_df = spark.read.format("delta").load(sourceFilePath)

# A missing column means the silver contract was broken upstream, which is a pipeline error, not bad data
missing_cols = [c for c in REQUIRED_COLS if c not in silver_df.columns]
if missing_cols:
    raise Exception(f"Silver table is missing required columns: {missing_cols}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = silver_df.filter(
        (col("Order_Year") == int(p_year)) &
        (col("Order_Month") == int(p_month))
    )

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------- Validation 3: row level checks ----------------
# Staging is overwritten on every run, so an empty month still flows through and clears stale staging data.
checks = []

def check(condition, message):
    checks.append(when(condition, lit(message)))

def is_blank(c):
    return col(c).isNull() | (trim(col(c).cast("string")) == "")

for c in FACT_KEY_COLS:
    check(is_blank(c), f"Missing mandatory value: {c}")

check(col("Quantity").isNull() | (col("Quantity") <= 0), "Quantity must be greater than 0")
check(col("Unit_Price").isNull() | (col("Unit_Price") < 0), "Unit_Price missing or negative")
check(col("Return_Quantity") < 0, "Return_Quantity cannot be negative")
check(col("Return_Quantity") > col("Quantity"), "Return_Quantity exceeds Quantity")

errors = concat_ws("; ", *checks)
df = df.withColumn(
    "Validation_Errors",
    when(errors == "", lit(None).cast("string")).otherwise(errors)
)

# Duplicate Order_Line_ID: keep one row, quarantine the rest so the fact table stays unique
dedupe_window = Window.partitionBy("Order_Line_ID").orderBy(
    when(col("Validation_Errors").isNull(), 0).otherwise(1),
    col("Return_Date").desc_nulls_last()
)
df = df.withColumn("_rn", row_number().over(dedupe_window))
df = df.withColumn(
    "Validation_Errors",
    when(
        (col("_rn") > 1) & ~is_blank("Order_Line_ID"),
        concat_ws("; ", col("Validation_Errors"), lit("Duplicate Order_Line_ID in silver"))
    ).otherwise(col("Validation_Errors"))
).drop("_rn")

df = df.persist(StorageLevel.MEMORY_AND_DISK)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------- Split valid vs quarantine ----------------
silver_columns = silver_df.columns

valid_df = df.filter(col("Validation_Errors").isNull()).select(*silver_columns)

quarantine_df = df.filter(col("Validation_Errors").isNotNull()).select(
    *silver_columns,
    col("Validation_Errors"),
    current_timestamp().alias("Quarantined_At")
)

# One pass gives the totals AND the expected row count of every staging table
# (each dimension has one row per distinct key; countDistinct ignores NULLs, matching dropna on Return_Reason)
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
quarantine_count = input_count - valid_count
expected_counts = {
    "Fact_Orders_stg": valid_count,
    "Dim_Product_stg": c["products"],
    "Dim_Customer_stg": c["customers"],
    "Dim_Channel_stg": c["channels"],
    "Dim_Return_Reason_stg": c["return_reasons"],
    "Dim_Payment_Method_stg": c["payment_methods"],
}

# Overwrite so a re-run of the same month does not duplicate quarantined rows
quarantine_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(quarantinePath)

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

# CELL ********************

fact_orders_df = valid_df.drop(
                    "Customer_Segment",
                    "Customer_City",
                    "Customer_State",
                    "Product_Name",
                    "Category",
                    "Subcategory",
                    "Brand"
                )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_products_df = valid_df.select(
                        "Product_ID",
                        "Product_Name",
                        "Category",
                        "Subcategory",
                        "Brand"
                    ).drop_duplicates(["Product_ID"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_customers_df = valid_df.select(
                        "Customer_ID",
                        "Customer_Segment",
                        "Customer_City",
                        "Customer_State"
                    ).drop_duplicates(["Customer_ID"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_channel_df = valid_df.select(
                        "Sales_Channel"
                    ).drop_duplicates(["Sales_Channel"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_return_reason_df = valid_df.select(
                        "Return_Reason"
                    ).drop_duplicates(["Return_Reason"]) \
                    .dropna(subset=["Return_Reason"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_payment_method_df = valid_df.select(
                                "Payment_Method"
                            ).drop_duplicates(["Payment_Method"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------- Validation 4: dimension consistency (warnings only) ----------------
# drop_duplicates picks an arbitrary row per ID, so flag IDs that carry conflicting attributes.
def report_conflicts(frame, key, attrs, name):
    conflicts = frame.select(key, *attrs).distinct() \
                     .groupBy(key).count().filter(col("count") > 1).count()
    if conflicts > 0:
        print(f"WARNING: {conflicts} {key} value(s) have conflicting {name} attributes in this batch")

report_conflicts(valid_df, "Product_ID",
                 ["Product_Name", "Category", "Subcategory", "Brand"], "product")
report_conflicts(valid_df, "Customer_ID",
                 ["Customer_Segment", "Customer_City", "Customer_State"], "customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_orders_df.printSchema()
dim_products_df.printSchema()
dim_customers_df.printSchema()
dim_channel_df.printSchema()
dim_return_reason_df.printSchema()
dim_payment_method_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------- Validation 5: staging write with read-back check ----------------
def write_to_staging(df, table_name):
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

# CELL ********************

fact_row_count = write_to_staging(fact_orders_df, "Fact_Orders_stg")
write_to_staging(dim_products_df, "Dim_Product_stg")
write_to_staging(dim_customers_df, "Dim_Customer_stg")
write_to_staging(dim_channel_df, "Dim_Channel_stg")
write_to_staging(dim_return_reason_df, "Dim_Return_Reason_stg")
write_to_staging(dim_payment_method_df, "Dim_Payment_Method_stg")

print(f"Quarantined rows: {quarantine_count}")

df.unpersist()

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
