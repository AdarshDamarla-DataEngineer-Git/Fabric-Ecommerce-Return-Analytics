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
Target_Warehouse = "Ecommerce_Returns_Warehouse"
Target_Schema = "Returns_Staging"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("delta").load(sourceFilePath).filter(
        (col("Order_Year") == int(p_year)) &
        (col("Order_Month") == int(p_month))
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

fact_orders_df = df.drop(
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

dim_products_df = df.select(
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

dim_customers_df = df.select(
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

dim_channel_df = df.select(
                        "Sales_Channel"
                    ).drop_duplicates(["Sales_Channel"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_return_reason_df = df.select(
                        "Return_Reason"
                    ).drop_duplicates(["Return_Reason"]) \
                    .dropna(subset=["Return_Reason"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_payment_method_df = df.select(
                                "Payment_Method"
                            ).drop_duplicates(["Payment_Method"]) 

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

def write_to_staging(df, table_name):
    target_table = f"{Target_Warehouse}.{Target_Schema}.{table_name}"

    row_count = df.count()

    df.write \
        .mode("overwrite") \
        .synapsesql(target_table)

    print(f"Successfully loaded: {target_table}")
    print(f"Rows loaded: {row_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

write_to_staging(fact_orders_df, "Fact_Orders_stg")
write_to_staging(dim_products_df, "Dim_Product_stg")
write_to_staging(dim_customers_df, "Dim_Customer_stg")
write_to_staging(dim_channel_df, "Dim_Channel_stg")
write_to_staging(dim_return_reason_df, "Dim_Return_Reason_stg")
write_to_staging(dim_payment_method_df, "Dim_Payment_Method_stg")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(str(fact_orders_df.count()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
