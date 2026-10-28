CREATE   PROCEDURE Returns_Staging.Merge_Returns_Gold_Final
AS
BEGIN

    -- All six merges succeed together or none of them are kept.
    -- On any error the transaction is rolled back and the error is re-raised,
    -- so the pipeline still fails, logs the real message and sends the email.
    BEGIN TRY

        BEGIN TRANSACTION;

            --   1. PRODUCT DIMENSION
            MERGE Returns_Gold.Dim_Product AS T
            USING Returns_Staging.Dim_Product_stg AS S
                ON T.Product_ID = S.Product_ID

            WHEN MATCHED THEN
                UPDATE SET
                    T.Product_Name = S.Product_Name,
                    T.Category = S.Category,
                    T.Subcategory = S.Subcategory,
                    T.Brand = S.Brand

            WHEN NOT MATCHED THEN
                INSERT
                (
                    Product_ID,
                    Product_Name,
                    Category,
                    Subcategory,
                    Brand
                )
                VALUES
                (
                    S.Product_ID,
                    S.Product_Name,
                    S.Category,
                    S.Subcategory,
                    S.Brand
                );



            --   2. CUSTOMER DIMENSION
            MERGE Returns_Gold.Dim_Customer AS T
            USING Returns_Staging.Dim_Customer_stg AS S
                ON T.Customer_ID = S.Customer_ID

            WHEN MATCHED THEN
                UPDATE SET
                    T.Customer_Segment = S.Customer_Segment,
                    T.Customer_City = S.Customer_City,
                    T.Customer_State = S.Customer_State

            WHEN NOT MATCHED THEN
                INSERT
                (
                    Customer_ID,
                    Customer_Segment,
                    Customer_City,
                    Customer_State
                )
                VALUES
                (
                    S.Customer_ID,
                    S.Customer_Segment,
                    S.Customer_City,
                    S.Customer_State
                );


            --   3. SALES CHANNEL DIMENSION
            MERGE Returns_Gold.Dim_Channel AS T
            USING Returns_Staging.Dim_Channel_stg AS S
                ON T.Sales_Channel = S.Sales_Channel

            WHEN NOT MATCHED THEN
                INSERT (Sales_Channel)
                VALUES (S.Sales_Channel);


            --   4. RETURN REASON DIMENSION
            MERGE Returns_Gold.Dim_Return_Reason AS T
            USING Returns_Staging.Dim_Return_Reason_stg AS S
                ON T.Return_Reason = S.Return_Reason

            WHEN NOT MATCHED THEN
                INSERT (Return_Reason)
                VALUES (S.Return_Reason);



            --   5. PAYMENT METHOD DIMENSION
            MERGE Returns_Gold.Dim_Payment_Method AS T
            USING Returns_Staging.Dim_Payment_Method_stg AS S
                ON T.Payment_Method = S.Payment_Method

            WHEN NOT MATCHED THEN
                INSERT (Payment_Method)
                VALUES (S.Payment_Method);



            --   6. FACT ORDERS
            MERGE Returns_Gold.Fact_Orders AS T
            USING Returns_Staging.Fact_Orders_stg AS S
                ON T.Order_Line_ID = S.Order_Line_ID

            WHEN MATCHED THEN
                UPDATE SET
                    T.Order_ID = S.Order_ID,
                    T.Order_Date = S.Order_Date,
                    T.Customer_ID = S.Customer_ID,
                    T.Product_ID = S.Product_ID,
                    T.Sales_Channel = S.Sales_Channel,
                    T.Payment_Method = S.Payment_Method,
                    T.Quantity = S.Quantity,
                    T.Unit_Price = S.Unit_Price,
                    T.Unit_Cost = S.Unit_Cost,
                    T.Discount_Pct = S.Discount_Pct,
                    T.Shipping_Fee = S.Shipping_Fee,
                    T.Delivery_Date = S.Delivery_Date,
                    T.Return_Flag = S.Return_Flag,
                    T.Return_Date = S.Return_Date,
                    T.Return_Reason = S.Return_Reason,
                    T.Return_Quantity = S.Return_Quantity,
                    T.Refund_Deduction_Pct = S.Refund_Deduction_Pct,
                    T.Product_Rating = S.Product_Rating,
                    T.Order_Year = S.Order_Year,
                    T.Order_Month = S.Order_Month

            WHEN NOT MATCHED THEN
                INSERT
                (
                    Order_Line_ID,
                    Order_ID,
                    Order_Date,
                    Customer_ID,
                    Product_ID,
                    Sales_Channel,
                    Payment_Method,
                    Quantity,
                    Unit_Price,
                    Unit_Cost,
                    Discount_Pct,
                    Shipping_Fee,
                    Delivery_Date,
                    Return_Flag,
                    Return_Date,
                    Return_Reason,
                    Return_Quantity,
                    Refund_Deduction_Pct,
                    Product_Rating,
                    Order_Year,
                    Order_Month
                )
                VALUES
                (
                    S.Order_Line_ID,
                    S.Order_ID,
                    S.Order_Date,
                    S.Customer_ID,
                    S.Product_ID,
                    S.Sales_Channel,
                    S.Payment_Method,
                    S.Quantity,
                    S.Unit_Price,
                    S.Unit_Cost,
                    S.Discount_Pct,
                    S.Shipping_Fee,
                    S.Delivery_Date,
                    S.Return_Flag,
                    S.Return_Date,
                    S.Return_Reason,
                    S.Return_Quantity,
                    S.Refund_Deduction_Pct,
                    S.Product_Rating,
                    S.Order_Year,
                    S.Order_Month
                );

        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH

        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;

    END CATCH

END;

GO