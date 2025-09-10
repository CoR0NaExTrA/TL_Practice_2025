{{
    config(
        materialized='table',
        alias='dim_customer_category',
        unique_key='customer_category_id'
    )
}}

SELECT
    CustomerCategoryID AS customer_category_id, --business key/PK
    CustomerCategoryName AS customer_category_name
FROM {{ ref('CustomerCategory') }}