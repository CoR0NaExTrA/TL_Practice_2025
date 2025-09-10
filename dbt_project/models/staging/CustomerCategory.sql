{{
  config(
    materialized='view',
    alias='stg_customer_category'
  )
}}

SELECT *
FROM {{ source('staging', 'CustomerCategory') }}