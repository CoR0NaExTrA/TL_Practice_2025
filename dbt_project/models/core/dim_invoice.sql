{{
    config(
        materialized='view',
        alias='dim_invoice'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['InvoiceID']) }} AS invoice_key, --PK/surrogate key
    InvoiceID AS invoice_id, --business key
    c.customer_key AS customer_key, --FK
    BillToCustomerID AS bill_to_customer_id,
    o.order_key AS order_key, --FK
    DeliveryMethodID AS delivery_method_id,
    ContactPersonID AS contact_person_id,
    AccountsPersonID AS accounts_person_id,
    SalespersonPersonID AS salesperson_person_id,
    PackedByPersonID AS packed_by_person_id,
    InvoiceDate AS invoice_date,
    CustomerPurchaseOrderNumber AS customer_purchase_order_number,
    DeliveryInstructions AS delivery_instructions,
    TotalDryItems AS total_dry_items,
    TotalChillerItems AS total_chiller_items,
    ConfirmedDeliveryTime AS confirmed_delivery_time, 
    ConfirmedReceivedBy AS confirmed_received_by
FROM {{ ref('Invoice') }} i
JOIN {{ ref('dim_customer') }} c ON c.customer_key = i.customer_key
JOIN {{ ref('dim_order') }} o ON o.order_key = i.order_key