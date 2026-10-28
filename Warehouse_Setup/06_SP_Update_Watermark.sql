-- 06_SP_Update_Watermark.sql
-- Moves the watermark forward only (never backwards). Unchanged from MyQueries/SP_Update_Watermark.sql.

CREATE OR ALTER PROCEDURE Returns_Control.Update_Watermark
    @Pipeline_Name   VARCHAR(100),
    @Process_Year    INT,
    @Process_Month   INT,
    @Pipeline_Run_ID VARCHAR(100)
AS
BEGIN

    UPDATE Returns_Control.Pipeline_Watermark
    SET
        Last_Processed_Year  = @Process_Year,
        Last_Processed_Month = @Process_Month,
        Last_Success_Run_ID  = @Pipeline_Run_ID,
        Last_Updated_Date    = GETDATE()
    WHERE Pipeline_Name = @Pipeline_Name
      AND (Last_Processed_Year * 100 + Last_Processed_Month)
          < (@Process_Year * 100 + @Process_Month);

END;
