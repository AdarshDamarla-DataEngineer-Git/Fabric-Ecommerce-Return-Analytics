CREATE TABLE [Returns_Control].[Pipeline_Metadata] (
    [Pipeline_Name]                  VARCHAR (100)  NOT NULL,
    [Pipeline_Run_ID]                VARCHAR (100)  NOT NULL,
    [Pipeline_Trigger_Type]          VARCHAR (50)   NULL,
    [Process_Year]                   INT            NOT NULL,
    [Process_Month]                  INT            NOT NULL,
    [Process_Month_Name]             VARCHAR (20)   NULL,
    [Source_File_Name]               VARCHAR (300)  NULL,
    [Bronze_Record_Processed]        BIGINT         NULL,
    [Silver_Record_Processed]        BIGINT         NULL,
    [Gold_Fact_Stg_Record_Processed] BIGINT         NULL,
    [Gold_Fact_Final_Record_Count]   BIGINT         NULL,
    [Start_Time]                     DATETIME2 (6)  NULL,
    [End_Time]                       DATETIME2 (6)  NULL,
    [Duration_Seconds]               BIGINT         NULL,
    [Load_Status]                    VARCHAR (20)   NULL,
    [Error_Message]                  VARCHAR (2000) NULL
);


GO