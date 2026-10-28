-- 99_Reset_All_Tables.sql
-- !!! DESTRUCTIVE: DELETES ALL DATA in Gold, staging and the control tables. !!!
-- Use only on a development warehouse, or when you really want to reload everything.
-- It does NOT touch the Lakehouse (Bronze, Silver, quarantine files).
--
-- After running it, the next pipeline run starts again from the month set in the
-- re-seed step at the bottom (default: loads January 2025 first).

-- Gold
TRUNCATE TABLE Returns_Gold.Fact_Orders;
TRUNCATE TABLE Returns_Gold.Dim_Product;
TRUNCATE TABLE Returns_Gold.Dim_Customer;
TRUNCATE TABLE Returns_Gold.Dim_Channel;
TRUNCATE TABLE Returns_Gold.Dim_Payment_Method;
TRUNCATE TABLE Returns_Gold.Dim_Return_Reason;

-- Staging
TRUNCATE TABLE Returns_Staging.Fact_Orders_stg;
TRUNCATE TABLE Returns_Staging.Dim_Product_stg;
TRUNCATE TABLE Returns_Staging.Dim_Customer_stg;
TRUNCATE TABLE Returns_Staging.Dim_Channel_stg;
TRUNCATE TABLE Returns_Staging.Dim_Payment_Method_stg;
TRUNCATE TABLE Returns_Staging.Dim_Return_Reason_stg;

-- Control
TRUNCATE TABLE Returns_Control.Pipeline_Metadata;
TRUNCATE TABLE Returns_Control.Pipeline_Watermark;

-- Re-seed the watermark. Without a row here the pipeline's Lookup_Watermark step fails.
-- Set these to the month BEFORE the first month to load.
DECLARE @Start_Year  INT = 2024;
DECLARE @Start_Month INT = 12;

INSERT INTO Returns_Control.Pipeline_Watermark
(
    Pipeline_Name, Last_Processed_Year, Last_Processed_Month, Last_Success_Run_ID, Last_Updated_Date
)
VALUES
(
    'Ecommerce_Returns_Pipeline', @Start_Year, @Start_Month, NULL, GETDATE()
);
