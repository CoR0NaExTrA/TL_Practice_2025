{{
    config(
        materialized='view',
        alias='stg_smart_category'
    )
}}

WITH cleared AS (
    SELECT
        CAST(id AS INT) AS smart_category_id,
        CAST(entitytypeid AS INT) AS entitytypeid,

        CAST(name AS VARCHAR(255)) AS name,
        CAST(sort AS INT) as sort,

        CASE 
            WHEN isdefault = 'Y' OR isdefault = '1' THEN 1
            WHEN isdefault = 'N' OR isdefault = '0' OR isdefault = '' THEN 0
            ELSE NULL 
        END AS is_default
    FROM {{ source('landing', 'test_db.crm_process_smart_category') }}
    WHERE id IS NOT NULL
)

SELECT * FROM cleared