{{
    config(
        materialized='table',
        alias='dim_customer',
        unique_key='customer_id'
    )
}}

SELECT
    CustomerID AS customer_id, --business key/PK
    CustomerName AS customer_name,
    BillToCustomerID AS bill_to_customer_id,
    cc.CustomerCategoryID AS customer_category_id, --FK
    b.BuyingGroupID AS buying_group_id, --FK
    PrimaryContactPersonID AS primary_contact_person_id,
    AlternateContactPersonID AS alternate_contact_person_id,
    DeliveryMethodID AS delivery_method_id,
    DeliveryCityID AS delivery_city_id,
    CreditLimit AS credit_limit,
    AccountOpenedDate AS account_opened_date,
    StandardDiscountPercentage AS standard_discount_percentage,
    IsStatementSent AS is_statement_sent,
    IsOnCreditHold AS is_on_credit_hold,
    PaymentDays AS payment_days,
    PhoneNumber AS phone_number,
    WebsiteURL AS website_url,
    DeliveryAddressLine AS delivery_address_line,
    DeliveryLatitude AS delivery_latitude,
    dbt_loaded_at
FROM {{ ref('Customer') }} c
JOIN {{ ref('BuyingGroup') }} b ON b.BuyingGroupID = c.BuyingGroupID
JOIN {{ ref('CustomerCategory') }} cc ON cc.CustomerCategoryID = c.CustomerCategoryID