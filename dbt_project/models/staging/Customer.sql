{{
    config(
        materialized='view',
        alias='stg_customer'
    )
}}
WITH cleaned AS (
    SELECT
        TRY_CAST(CustomerID AS INT) AS CustomerID,
        TRIM(CustomerName) AS CustomerName,

        TRY_CAST(BillToCustomerID AS INT) AS BillToCustomerID,
        TRY_CAST(CustomerCategoryID AS INT) AS CustomerCategoryID,
        TRY_CAST(BuyingGroupID AS INT) AS BuyingGroupID,
        TRY_CAST(PrimaryContactPersonID AS INT) AS PrimaryContactPersonID,
        TRY_CAST(AlternateContactPersonID AS INT) AS AlternateContactPersonID,
        TRY_CAST(DeliveryMethodID AS INT) AS DeliveryMethodID,
        TRY_CAST(DeliveryCityID AS INT) AS DeliveryCityID,

        TRY_CAST(CreditLimit AS MONEY) AS CreditLimit,
        CASE 
            WHEN TRY_CAST(StandardDiscountPercentage AS REAL) BETWEEN 0 AND 100 
            THEN TRY_CAST(StandardDiscountPercentage AS REAL)
            ELSE NULL
        END AS StandardDiscountPercentage,

        TRY_CONVERT(DATE, AccountOpenedDate, 120) AS AccountOpenedDate,

        CASE 
        WHEN TRY_CAST(IsStatementSent AS BIT) = 1 
            THEN 1 
            ELSE 0 
        END AS IsStatementSent,

        CASE 
        WHEN TRY_CAST(IsOnCreditHold AS BIT) = 1 
            THEN 1 
            ELSE 0 
        END AS IsOnCreditHold,

        TRY_CAST(PaymentDays AS INT) AS PaymentDays,
        --если получится то можно REGEXP_REPLACE 
        TRIM(PhoneNumber) AS PhoneNumber,
        TRIM(WebsiteURL) AS WebsiteURL,

        TRIM(DeliveryAddressLine) AS DeliveryAddressLine,
        CASE 
            WHEN TRY_CAST(DeliveryLocationLat AS DECIMAL(9,6)) BETWEEN -90 AND 90 
            THEN TRY_CAST(DeliveryLocationLat AS DECIMAL(9,6))
            ELSE NULL
        END AS DeliveryLatitude,

        CURRENT_TIMESTAMP AS dbt_loaded_at
    FROM {{ source('landing', 'Customer') }}
)
SELECT * FROM cleaned
WHERE CustomerID IS NOT NULL