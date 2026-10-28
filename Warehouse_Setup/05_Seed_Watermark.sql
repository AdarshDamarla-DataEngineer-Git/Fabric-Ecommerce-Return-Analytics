-- 05_Seed_Watermark.sql
-- Creates the starting watermark row for the pipeline.
-- Set @Start_Year / @Start_Month to the month BEFORE the first month you want to load.
-- Example: to load January 2025 first, use 2024 and 12.
-- Safe to re-run: the row is inserted only if the pipeline has no watermark yet.

DECLARE @Start_Year  INT = 2024;
DECLARE @Start_Month INT = 12;

IF NOT EXISTS (
    SELECT 1
    FROM Returns_Control.Pipeline_Watermark
    WHERE Pipeline_Name = 'Ecommerce_Returns_Pipeline'
)
INSERT INTO Returns_Control.Pipeline_Watermark
(
    Pipeline_Name,
    Last_Processed_Year,
    Last_Processed_Month,
    Last_Success_Run_ID,
    Last_Updated_Date
)
VALUES
(
    'Ecommerce_Returns_Pipeline',
    @Start_Year,
    @Start_Month,
    NULL,
    GETDATE()
);
