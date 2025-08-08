{{
    config(
        materialized='view',
        alias='dim_order'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['InvoiceID']) }} AS order_key, --PK/surrogate key
    InvoiceID AS order_id, --business key
    c.customer_key AS customer_key, --FK
    SalespersonPersonID AS salesperson_person_id,
    PickedByPersonID AS picked_by_person_id,
    ContactPersonID AS contact_person_id,
    BackorderOrderID AS backorder_order_id,
    OrderDate AS order_date,
    ExpectedDeliveryDate AS expected_delivery_date,
    CustomerPurchaseOrderNumber AS customer_purchase_order_number,
    IsUndersupplyBackordered AS is_undersupply_backordered,
    PickingCompletedWhen AS picking_completed_when
    
FROM {{ ref('Invoice') }} i
JOIN {{ ref('dim_customer') }} c ON c.customer_key = i.customer_key