CREATE   PROCEDURE Returns_Control.Update_Watermark
    @Pipeline_Name VARCHAR(100),
    @Process_Year INT,
    @Process_Month INT,
    @Pipeline_Run_ID VARCHAR(100)
AS
BEGIN

    UPDATE Returns_Control.Pipeline_Watermark
    SET
        Last_Processed_Year = @Process_Year,
        Last_Processed_Month = @Process_Month,
        Last_Success_Run_ID = @Pipeline_Run_ID,
        Last_Updated_Date = GETDATE()
    WHERE Pipeline_Name = @Pipeline_Name;

END;

GO