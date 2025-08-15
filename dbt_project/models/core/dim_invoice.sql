{{
    config(
        materialized='table',
        alias='dim_invoice',
        unique_key='invoice_id'
    )
}}

SELECT
    i.InvoiceID AS invoice_id, --business key/PK
    c.CustomerID AS customer_id, --FK
    i.BillToCustomerID AS bill_to_customer_id,
    o.OrderID AS order_id, --FK
    i.DeliveryMethodID AS delivery_method_id,
    i.ContactPersonID AS contact_person_id,
    i.AccountsPersonID AS accounts_person_id,
    i.SalespersonPersonID AS salesperson_person_id,
    i.PackedByPersonID AS packed_by_person_id,
    i.InvoiceDate AS invoice_date,
    i.CustomerPurchaseOrderNumber AS customer_purchase_order_number,
    i.DeliveryInstructions AS delivery_instructions,
    i.TotalDryItems AS total_dry_items,
    i.TotalChillerItems AS total_chiller_items,
    i.ConfirmedDeliveryTime AS confirmed_delivery_time, 
    i.ConfirmedReceivedBy AS confirmed_received_by
FROM {{ ref('Invoice') }} i
JOIN {{ ref('Customer') }} c ON c.CustomerID = i.CustomerID
JOIN {{ ref('Order') }} o ON o.OrderID = i.OrderID