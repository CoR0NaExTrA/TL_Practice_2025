{{
    config(
        materialized='table',
        alias='fact_sales',
        unique_key='sales_line_id'
    )
}}

WITH invoice_lines AS (
    SELECT
        InvoiceLineID AS sales_line_id,
        InvoiceID AS document_id,
        StockItemID,
        PackageTypeID,
        [Description],
        Quantity,
        UnitPrice,
        TaxRate,
        TaxAmount,
        LineProfit,
        ExtendedPrice,
        NULL AS PickedQuantity,
        NULL AS PickingCompletedWhen,
        'invoice' AS document_type
    FROM {{ ref('InvoiceLine') }}
),

order_lines AS (
    SELECT
        OrderLineID AS sales_line_id,
        OrderID AS document_id,
        StockItemID,
        PackageTypeID,
        [Description],
        Quantity,
        UnitPrice,
        TaxRate,
        NULL AS TaxAmount,
        NULL AS LineProfit,
        NULL AS ExtendedPrice,
        PickedQuantity,
        PickingCompletedWhen,
        'order' AS document_type
    FROM {{ ref('OrderLine') }}
)

SELECT
    sales_line_id,
    document_id,
    document_type,
    StockItemID,
    PackageTypeID,
    [Description],
    Quantity,
    UnitPrice,
    TaxRate,
    TaxAmount,
    LineProfit,
    ExtendedPrice,
    PickedQuantity,
    PickingCompletedWhen,
    CASE 
        WHEN document_type = 'invoice' THEN ExtendedPrice
        WHEN document_type = 'order' THEN Quantity * UnitPrice
    END AS calculated_amount,
    CASE 
        WHEN document_type = 'invoice' THEN TaxAmount
        WHEN document_type = 'order' THEN Quantity * UnitPrice * (TaxRate / 100)
    END AS calculated_tax_amount
FROM (
    SELECT * FROM invoice_lines
    UNION ALL
    SELECT * FROM order_lines
) AS combined