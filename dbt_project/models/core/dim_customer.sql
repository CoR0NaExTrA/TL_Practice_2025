{{
    config(
        materialized='view',
        alias='dim_customer'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['CustomerID']) }} AS customer_key, --PK/surrogate key
    CustomerID AS customer_id, --business key
    CustomerName AS customer_name,
    BillToCustomerID AS bill_to_customer_id,
    cc.customer_category_key AS customer_category_key, --FK
    b.buying_group_key AS buying_group_key, --FK
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
    DeliveryLocationLat AS delivery_location_lat,
FROM {{ ref('Customer') }} c
JOIN {{ ref('dim_buying_group') }} b ON b.buying_group_key = c.buying_group_key
JOIN {{ ref('dim_customer_category') }} cc ON cc.customer_category_key = c.customer_category_key