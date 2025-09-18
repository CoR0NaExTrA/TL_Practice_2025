{{
    config(
        materialized='view',
        alias='stg_product_section'
    )
}}

WITH cleared AS (
    SELECT
        CAST(id AS INT) AS product_section_id,
        CAST(catalog_id AS INT) AS catalog_id,
        CAST(section_id AS INT) as section_id,
        CAST(xml_id AS VARCHAR(255)) as xml_id,

        CAST(name AS VARCHAR(255)) AS name,
        CAST(code AS VARCHAR(255)) as code
    FROM {{ source('landing', 'test_db.crm_process_product_section') }}
    WHERE id IS NOT NULL
)

SELECT * FROM cleared