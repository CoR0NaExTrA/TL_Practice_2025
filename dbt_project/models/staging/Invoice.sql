{{
    config(
        materialized='view',
        alias='stg_invoice'
    )
}}

WITH cleaned AS(
    SELECT
        TRY_CAST(InvoiceID AS INT) AS InvoiceID,

        TRY_CAST(CustomerID AS INT) AS CustomerID,
        TRY_CAST(BillToCustomerID AS INT) AS BillToCustomerID,
        TRY_CAST(OrderID AS INT) AS OrderID,
        TRY_CAST(DeliveryMethodID AS INT) AS DeliveryMethodID,
        TRY_CAST(ContactPersonID AS INT) AS ContactPersonID,
        TRY_CAST(AccountsPersonID AS INT) AS AccountsPersonID,
        TRY_CAST(SalespersonPersonID AS INT) AS SalespersonPersonID,
        TRY_CAST(PackedByPersonID AS INT) AS PackedByPersonID,

        TRY_CONVERT(DATE, InvoiceDate, 120) AS InvoiceDate,

        TRY_CAST(CustomerPurchaseOrderNumber AS INT) AS CustomerPurchaseOrderNumber,

        TRIM(DeliveryInstructions) AS DeliveryInstructions,

        TRY_CAST(TotalDryItems AS INT) AS TotalDryItems,
        TRY_CAST(TotalChillerItems AS INT) AS TotalChillerItems,

        TRY_CONVERT(DATETIME2, ConfirmedDeliveryTime, 121) AS ConfirmedDeliveryTime,
        TRIM(ConfirmedReceivedBy) AS ConfirmedReceivedBy
    FROM {{ source('staging', 'Invoice') }}
)
SELECT * FROM cleaned
WHERE InvoiceID IS NOT NULL