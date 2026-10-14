CREATE TABLE [Returns_Control].[Pipeline_Watermark] (
    [Pipeline_Name]        VARCHAR (100) NOT NULL,
    [Last_Processed_Year]  INT           NOT NULL,
    [Last_Processed_Month] INT           NOT NULL,
    [Last_Success_Run_ID]  VARCHAR (100) NULL,
    [Last_Updated_Date]    DATETIME2 (6) NULL
);


GO