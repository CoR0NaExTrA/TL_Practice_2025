{{
    config(
        materialized='view',
        alias='dim_buying_group'
    )
}}

SELECT 
    {{dbt_utils.generate_surrogate_key(['BuyingGroupID'])}} AS buying_group_key, --PK/surrogate key
    BuyingGroupID AS buying_group_id, --business key
    BuyingGroupName AS buying_group_name
FROM {{ source('landing', 'CustomerCategory') }}