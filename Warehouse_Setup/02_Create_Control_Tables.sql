-- 02_Create_Control_Tables.sql
-- Control tables used for incremental loading and run auditing.
-- Safe to re-run: each table is created only if it does not already exist.

IF OBJECT_ID('Returns_Control.Pipeline_Watermark', 'U') IS NULL
CREATE TABLE Returns_Control.Pipeline_Watermark
(
    Pipeline_Name           VARCHAR(100) NOT NULL,
    Last_Processed_Year     INT NOT NULL,
    Last_Processed_Month    INT NOT NULL,
    Last_Success_Run_ID     VARCHAR(100),
    Last_Updated_Date       DATETIME2(6)
);

IF OBJECT_ID('Returns_Control.Pipeline_Metadata', 'U') IS NULL
CREATE TABLE Returns_Control.Pipeline_Metadata
(
    Pipeline_Name           VARCHAR(100) NOT NULL,
    Pipeline_Run_ID         VARCHAR(100) NOT NULL,
    Pipeline_Trigger_Type   VARCHAR(50),

    Process_Year            INT NOT NULL,
    Process_Month           INT NOT NULL,
    Process_Month_Name      VARCHAR(20),

    Source_File_Name        VARCHAR(300),

    Bronze_Record_Processed     BIGINT,
    Silver_Record_Processed     BIGINT,
    Gold_Fact_Stg_Record_Processed  BIGINT,
    Gold_Fact_Final_Record_Count  BIGINT,

    Start_Time              DATETIME2(6),
    End_Time                DATETIME2(6),
    Duration_Seconds        BIGINT,

    Load_Status             VARCHAR(20),
    Error_Message           VARCHAR(2000)
);
