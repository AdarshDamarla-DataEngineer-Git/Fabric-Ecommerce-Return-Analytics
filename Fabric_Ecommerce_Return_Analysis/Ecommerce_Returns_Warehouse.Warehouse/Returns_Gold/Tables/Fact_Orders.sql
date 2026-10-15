CREATE TABLE [Returns_Gold].[Fact_Orders] (
    [Order_Line_ID]        VARCHAR (100)   NULL,
    [Order_ID]             VARCHAR (100)   NULL,
    [Order_Date]           DATE            NULL,
    [Customer_ID]          VARCHAR (100)   NULL,
    [Product_ID]           VARCHAR (100)   NULL,
    [Sales_Channel]        VARCHAR (100)   NULL,
    [Payment_Method]       VARCHAR (100)   NULL,
    [Quantity]             INT             NULL,
    [Unit_Price]           DECIMAL (12, 2) NULL,
    [Unit_Cost]            DECIMAL (12, 2) NULL,
    [Discount_Pct]         DECIMAL (6, 2)  NULL,
    [Shipping_Fee]         DECIMAL (12, 2) NULL,
    [Delivery_Date]        DATE            NULL,
    [Return_Flag]          VARCHAR (20)    NULL,
    [Return_Date]          DATE            NULL,
    [Return_Reason]        VARCHAR (200)   NULL,
    [Return_Quantity]      INT             NULL,
    [Refund_Deduction_Pct] DECIMAL (6, 2)  NULL,
    [Product_Rating]       INT             NULL,
    [Order_Year]           INT             NULL,
    [Order_Month]          INT             NULL
);


GO