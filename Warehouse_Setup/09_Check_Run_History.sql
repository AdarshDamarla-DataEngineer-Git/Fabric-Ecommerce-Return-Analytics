-- 09_Check_Run_History.sql
-- Read-only checks. Safe to run at any time.

-- Latest runs
SELECT TOP 20
    Process_Year, Process_Month, Process_Month_Name, Load_Status,
    Bronze_Record_Processed, Silver_Record_Processed,
    Gold_Fact_Stg_Record_Processed, Gold_Fact_Final_Record_Count,
    Duration_Seconds, Pipeline_Trigger_Type, Start_Time, Error_Message
FROM Returns_Control.Pipeline_Metadata
ORDER BY Start_Time DESC;

-- Current watermark
SELECT *
FROM Returns_Control.Pipeline_Watermark;
