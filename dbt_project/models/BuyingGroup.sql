{{
  config(
    materialized='view',
    alias='stg_buying_group'
  )
}}

SELECT *
FROM {{ source('landing', 'BuyingGroup') }}