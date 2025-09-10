{{
    config(
        materialized='table',
        alias='dim_buying_group',
        unique_key='buying_group_id'
    )
}}

SELECT 
    BuyingGroupID AS buying_group_id, --business key/PK
    BuyingGroupName AS buying_group_name
FROM {{ ref('BuyingGroup') }}