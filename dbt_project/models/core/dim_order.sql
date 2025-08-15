{{
    config(
        materialized='table',
        alias='dim_order',
        unique_key='order_id'
    )
}}

SELECT
    OrderID AS order_id, --business key/PK
    c.CustomerID AS customer_id, --FK
    SalespersonPersonID AS salesperson_person_id,
    PickedByPersonID AS picked_by_person_id,
    ContactPersonID AS contact_person_id,
    BackorderOrderID AS backorder_order_id,
    OrderDate AS order_date,
    ExpectedDeliveryDate AS expected_delivery_date,
    CustomerPurchaseOrderNumber AS customer_purchase_order_number,
    IsUndersupplyBackordered AS is_undersupply_backordered,
    PickingCompletedWhen AS picking_completed_when
FROM {{ ref('Order') }} i
JOIN {{ ref('Customer') }} c ON c.CustomerID = i.CustomerID