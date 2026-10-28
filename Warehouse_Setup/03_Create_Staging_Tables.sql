-- 03_Create_Staging_Tables.sql
-- Staging tables. Overwritten by the Silver_Gold notebook on every run.
-- Safe to re-run: each table is created only if it does not already exist.

IF OBJECT_ID('Returns_Staging.Fact_Orders_stg', 'U') IS NULL
CREATE TABLE Returns_Staging.Fact_Orders_stg
(
    Order_Line_ID           VARCHAR(100),
    Order_ID                VARCHAR(100),
    Order_Date              DATE,
    Customer_ID             VARCHAR(100),
    Product_ID              VARCHAR(100),
    Sales_Channel           VARCHAR(100),
    Payment_Method          VARCHAR(100),

    Quantity                INT,
    Unit_Price              DECIMAL(12,2),
    Unit_Cost               DECIMAL(12,2),
    Discount_Pct            DECIMAL(6,2),
    Shipping_Fee            DECIMAL(12,2),

    Delivery_Date           DATE,
    Return_Flag             VARCHAR(20),
    Return_Date             DATE,
    Return_Reason           VARCHAR(200),
    Return_Quantity         INT,
    Refund_Deduction_Pct    DECIMAL(6,2),
    Product_Rating          INT,

    Order_Year              INT,
    Order_Month             INT
);

IF OBJECT_ID('Returns_Staging.Dim_Product_stg', 'U') IS NULL
CREATE TABLE Returns_Staging.Dim_Product_stg
(
    Product_ID      VARCHAR(100),
    Product_Name    VARCHAR(300),
    Category        VARCHAR(100),
    Subcategory     VARCHAR(100),
    Brand           VARCHAR(100)
);

IF OBJECT_ID('Returns_Staging.Dim_Customer_stg', 'U') IS NULL
CREATE TABLE Returns_Staging.Dim_Customer_stg
(
    Customer_ID         VARCHAR(100),
    Customer_Segment    VARCHAR(100),
    Customer_City       VARCHAR(100),
    Customer_State      VARCHAR(100)
);

IF OBJECT_ID('Returns_Staging.Dim_Channel_stg', 'U') IS NULL
CREATE TABLE Returns_Staging.Dim_Channel_stg
(
    Sales_Channel VARCHAR(100)
);

IF OBJECT_ID('Returns_Staging.Dim_Return_Reason_stg', 'U') IS NULL
CREATE TABLE Returns_Staging.Dim_Return_Reason_stg
(
    Return_Reason VARCHAR(200)
);

IF OBJECT_ID('Returns_Staging.Dim_Payment_Method_stg', 'U') IS NULL
CREATE TABLE Returns_Staging.Dim_Payment_Method_stg
(
    Payment_Method VARCHAR(100)
);
