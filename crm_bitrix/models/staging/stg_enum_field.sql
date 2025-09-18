{{
    config(
        materialized='view',
        alias='stg_enum_field'
    )
}}

WITH cleared AS (
    SELECT
        CAST(id AS INT) AS deal_id,
        CAST(value AS VARCHAR(255)) as value
    FROM {{ source('landing', 'test_db.crm_process_enum_field') }}
    WHERE id IS NOT NULL
)

SELECT * FROM cleared