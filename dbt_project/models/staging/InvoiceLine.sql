{{
    config(
        materialized='view',
        alias='stg_invoice_line'
    )
}}
WITH cleaned AS (
    SELECT
        TRY_CAST(InvoiceLineID AS INT) AS InvoiceLineID,

        TRY_CAST(InvoiceID AS INT) AS InvoiceID,
        TRY_CAST(StockItemID AS INT) AS StockItemID,
        TRY_CAST(PackageTypeID AS INT) AS PackageTypeID,

        TRIM([Description]) AS [Description],
        
        TRY_CAST(Quantity AS INT) AS Quantity,
        TRY_CAST(UnitPrice AS MONEY) AS UnitPrice,
        TRY_CAST(TaxRate AS MONEY) AS TaxRate,
        TRY_CAST(TaxAmount AS MONEY) AS TaxAmount,
        TRY_CAST(LineProfit AS MONEY) AS LineProfit,
        TRY_CAST(ExtendedPrice AS MONEY) AS ExtendedPrice
    FROM {{ source('staging', 'InvoiceLine') }}
)
SELECT * FROM cleaned
WHERE InvoiceLineID IS NOT NULL