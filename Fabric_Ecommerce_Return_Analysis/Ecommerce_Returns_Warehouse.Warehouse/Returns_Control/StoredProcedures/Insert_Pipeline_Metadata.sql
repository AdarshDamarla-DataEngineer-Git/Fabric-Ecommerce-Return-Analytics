CREATE   PROCEDURE Returns_Control.Insert_Pipeline_Metadata
    @Pipeline_Name           VARCHAR(100),
    @Pipeline_Run_ID         VARCHAR(100),
    @Pipeline_Trigger_Type   VARCHAR(50),

    @Process_Year            INT,
    @Process_Month           INT,
    @Process_Month_Name      VARCHAR(20),

    @Source_File_Name        VARCHAR(300),

    @Bronze_Record_Processed     BIGINT,
    @Silver_Record_Processed    BIGINT,
    @Gold_Fact_Stg_Record_Processed  BIGINT,

    @Start_Time              DATETIME2(6),

    @Error_Message           VARCHAR(2000) = NULL
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @End_Time DATETIME2(6);
    DECLARE @Duration_Seconds BIGINT;
    DECLARE @Load_Status VARCHAR(20);
    DECLARE @Gold_Fact_Final_Record_Count BIGINT;

    SET @End_Time = GETDATE();

    SET @Duration_Seconds =
        DATEDIFF(SECOND, @Start_Time, @End_Time);

    SET @Load_Status =
        CASE
            WHEN @Error_Message IS NULL
                OR LTRIM(RTRIM(@Error_Message)) = ''
            THEN 'Success'
            WHEN LTRIM(@Error_Message) LIKE 'Source file not found%'
            THEN 'No Source File'
            ELSE 'Failed'
        END;

    SELECT
        @Gold_Fact_Final_Record_Count = COUNT_BIG(*)
    FROM Returns_Gold.Fact_Orders;

    IF NOT EXISTS (
        SELECT 1
        FROM Returns_Control.Pipeline_Metadata
        WHERE Pipeline_Run_ID = @Pipeline_Run_ID
    )

    INSERT INTO Returns_Control.Pipeline_Metadata
    (
        Pipeline_Name,
        Pipeline_Run_ID,
        Pipeline_Trigger_Type,

        Process_Year,
        Process_Month,
        Process_Month_Name,

        Source_File_Name,

        Bronze_Record_Processed,
        Silver_Record_Processed,
        Gold_Fact_Stg_Record_Processed,
        Gold_Fact_Final_Record_Count,

        Start_Time,
        End_Time,
        Duration_Seconds,

        Load_Status,
        Error_Message
    )
    VALUES
    (
        @Pipeline_Name,
        @Pipeline_Run_ID,
        @Pipeline_Trigger_Type,

        @Process_Year,
        @Process_Month,
        @Process_Month_Name,

        @Source_File_Name,

        @Bronze_Record_Processed,
        @Silver_Record_Processed,
        @Gold_Fact_Stg_Record_Processed,
        @Gold_Fact_Final_Record_Count,

        @Start_Time,
        @End_Time,
        @Duration_Seconds,

        @Load_Status,
        @Error_Message
    );

END;

GO