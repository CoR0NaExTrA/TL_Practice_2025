{{
    config(
        materialized='view',
        alias='stg_order_line'
    )
}}

WITH cleaned AS (
    SELECT
        TRY_CAST(OrderLineID AS INT) AS OrderLineID,

        TRY_CAST(OrderID AS INT) AS OrderID,
        TRY_CAST(StockItemID AS INT) AS StockItemID,
        TRY_CAST(PackageTypeID AS INT) AS PackageTypeID,

        TRIM([Description]) AS [Description],

        TRY_CAST(Quantity AS INT) AS Quantity,
        TRY_CAST(UnitPrice AS MONEY) AS UnitPrice,
        TRY_CAST(TaxRate AS MONEY) AS TaxRate,
        TRY_CAST(PickedQuantity AS INT) AS PickedQuantity,

        TRY_CONVERT(DATE, PickingCompletedWhen, 121) AS PickingCompletedWhen
    FROM {{ source('staging', 'OrderLine') }}
)
SELECT * FROM cleaned
WHERE OrderLineID IS NOT NULL