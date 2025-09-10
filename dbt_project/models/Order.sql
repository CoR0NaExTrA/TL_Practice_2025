{{
    config(
        materialized='view',
        alias='stg_order'
    )
}}

WITH cleaned AS (
    SELECT
        TRY_CAST(OrderID AS INT) AS OrderID,
        
        TRY_CAST(CustomerID AS INT) AS CustomerID,
        TRY_CAST(SalespersonPersonID AS INT) AS SalespersonPersonID,
        TRY_CAST(PickedByPersonID AS INT) AS PickedByPersonID, 
        TRY_CAST(ContactPersonID AS INT) AS ContactPersonID,
        TRY_CAST(BackorderOrderID AS INT) AS BackorderOrderID,

        TRY_CONVERT(DATE, OrderDate, 120) AS OrderDate,
        TRY_CONVERT(DATE, ExpectedDeliveryDate, 120) AS ExpectedDeliveryDate,

        TRY_CAST(CustomerPurchaseOrderNumber AS INT) AS CustomerPurchaseOrderNumber,

        CASE 
        WHEN TRY_CAST(IsUndersupplyBackordered AS BIT) = 1 
            THEN 1 
            ELSE 0 
        END AS IsUndersupplyBackordered,

        TRY_CONVERT(DATETIME2, PickingCompletedWhen, 121) AS PickingCompletedWhen

    FROM {{ source('landing', 'Order') }}
)

SELECT * FROM cleaned
WHERE OrderID IS NOT NULL