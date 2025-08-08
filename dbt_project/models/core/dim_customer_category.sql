{{
    config(
        materialized='view',
        alias='dim_customer_category'
    )
}}

SELECT 
    {{dbt_utils.generate_surrogate_key(['CustomerCategoryID'])}} AS customer_category_key, --PK/surrogate key
    CustomerCategoryID AS customer_category_id, --business key
    CustomerCategoryName AS customer_category_name
FROM 