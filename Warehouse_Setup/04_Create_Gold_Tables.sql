-- 04_Create_Gold_Tables.sql
-- Gold tables. Star schema: one fact table and five dimensions.
-- Safe to re-run: each table is created only if it does not already exist.

IF OBJECT_ID('Returns_Gold.Fact_Orders', 'U') IS NULL
CREATE TABLE Returns_Gold.Fact_Orders
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

IF OBJECT_ID('Returns_Gold.Dim_Product', 'U') IS NULL
CREATE TABLE Returns_Gold.Dim_Product
(
    Product_ID      VARCHAR(100),
    Product_Name    VARCHAR(300),
    Category        VARCHAR(100),
    Subcategory     VARCHAR(100),
    Brand           VARCHAR(100)
);

IF OBJECT_ID('Returns_Gold.Dim_Customer', 'U') IS NULL
CREATE TABLE Returns_Gold.Dim_Customer
(
    Customer_ID         VARCHAR(100),
    Customer_Segment    VARCHAR(100),
    Customer_City       VARCHAR(100),
    Customer_State      VARCHAR(100)
);

IF OBJECT_ID('Returns_Gold.Dim_Channel', 'U') IS NULL
CREATE TABLE Returns_Gold.Dim_Channel
(
    Sales_Channel VARCHAR(100)
);

IF OBJECT_ID('Returns_Gold.Dim_Return_Reason', 'U') IS NULL
CREATE TABLE Returns_Gold.Dim_Return_Reason
(
    Return_Reason VARCHAR(200)
);

IF OBJECT_ID('Returns_Gold.Dim_Payment_Method', 'U') IS NULL
CREATE TABLE Returns_Gold.Dim_Payment_Method
(
    Payment_Method VARCHAR(100)
);
