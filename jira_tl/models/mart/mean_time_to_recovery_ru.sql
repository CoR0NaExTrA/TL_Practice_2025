{{ config (
    materialized='table'
    ) 
}}

SELECT *
FROM {{ ref('mean_time_to_recovery') }}
WHERE region = 'RU';