{{
    config(
        materialized='view',
        alias='dim_order'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['i.invoice_id', 'o.order_id', 'il.invoice_line_id', 'ol.order_line_id']) }} AS sales_key, --PK/surrogate key
    InvoiceLineID AS invoice_line_id, --business key
    OrderLineID AS order_line_id, --business key
    i.invoice_key AS invoice_key, --FK
    o.order_key AS order_key, --FK
    c.customer_key AS customer_key, --FK
    d.date_key AS date_key, --FK
    StockItemID AS stock_item_id,
    PackageTypeID AS package_type_id,
    Quantity AS quantity,
    UnitPrice AS unit_price,
    TaxRate AS tax_rate,
    TaxAmount AS tax_amount,
    LineProfit AS line_profit,
    ExpectedPrice AS extended_price,
    PickedQuantity AS picked_quantity,
    PickingCompletedWhen AS picking_completed_when
FROM {{ ref('Invoice') }} s
JOIN {{ ref('dim_invoice') }} i ON i.invoice_key = s.invoice_key
JOIN {{ ref('dim_order') }} o ON o.order_key = s.order_key
JOIN {{ ref('dim_customer') }} c ON c.customer_key = s.customer_key
JOIN {{ ref('dim_date') }} d ON d.date_key = s.date_key