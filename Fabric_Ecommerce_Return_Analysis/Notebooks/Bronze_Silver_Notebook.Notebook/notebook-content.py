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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.parquet(sourceFilePath)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn(
                "Unit_Price", 
                regexp_replace(col("Unit_Price"), r"[^0-9.-]", "")
            ).withColumn(
                "Unit_Cost", 
                regexp_replace(col("Unit_Cost"), r"[^0-9.-]", "")
            ).withColumn(
                "Discount_Pct", 
                regexp_replace(col("Discount_Pct"), r"[^0-9.-]", "")
            ).withColumn(
                "Shipping_Fee", 
                regexp_replace(col("Shipping_Fee"), r"[^0-9.-]", "")
            ).withColumn(
                "Refund_Deduction_Pct", 
                regexp_replace(col("Refund_Deduction_Pct"), r"[^0-9.-]", "")
            )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn(
        "Order_Date",
        to_date(col("Order_Date"), "dd-MMM-yyyy")
    ).withColumn(
        "Delivery_Date",
        to_date(col("Delivery_Date"), "dd-MMM-yyyy")
    ).withColumn(
        "Return_Date",
        to_date(col("Return_Date"), "dd-MMM-yyyy")
    ).withColumn(
        "Quantity",
        col("Quantity").cast("int")
    ).withColumn(
        "Return_Quantity",
        col("Return_Quantity").cast("int")
    ).withColumn(
        "Product_Rating",
        col("Product_Rating").cast("int")
    ).withColumn(
        "Unit_Price",
        col("Unit_Price").cast("decimal(12,2)")
    ).withColumn(
        "Unit_Cost",
        col("Unit_Cost").cast("decimal(12,2)")
    ).withColumn(
        "Discount_Pct",
        col("Discount_Pct").cast("decimal(6,2)")
    ).withColumn(
        "Shipping_Fee",
        col("Shipping_Fee").cast("decimal(12,2)")
    ).withColumn(
        "Refund_Deduction_Pct",
        col("Refund_Deduction_Pct").cast("decimal(6,2)")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn(
        "Sales_Channel",
        initcap(trim(col("Sales_Channel")))
    ).withColumn(
        "Payment_Method",
        initcap(trim(col("Payment_Method")))
    ).withColumn(
        "Return_Reason",
        initcap(trim(col("Return_Reason")))
    ).withColumn(
        "Payment_Method",
        regexp_replace(col("Payment_Method"), "Cod", "Cash On Delivery")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn(
        "Order_Year",
        year(col("Order_Date"))
    ).withColumn(
        "Order_Month",
        month(col("Order_Date"))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_record_processed_count = df.count()

if DeltaTable.isDeltaTable(spark, targetFilePath):

    silver_table = DeltaTable.forPath(spark, targetFilePath)

    silver_table.alias("target") \
        .merge(
            df.alias("source"),
            "target.Order_Line_ID = source.Order_Line_ID"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

else:
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("Order_Year", "Order_Month") \
        .save(targetFilePath) 

print(f"Rows loaded: {silver_record_processed_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Rows Processed in silver: {silver_record_processed_count}")
print(f"Total Rows in silver: {spark.read.format('delta').load(targetFilePath).count()}")

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
