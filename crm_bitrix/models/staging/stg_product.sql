{{
    config(
        materialized='view',
        alias='stg_product'
    )
}}

WITH cleared AS (
    SELECT
        CAST(id AS INT) AS product_id,
        CAST(xml_id AS INT) AS xml_id,
        CAST(modified_by AS INT) as modified_by,
        CAST(created_by AS INT) as created_by,
        CAST(catalog_id AS INT) as catalog_id,
        CAST(section_id AS INT) as section_id,
        CAST(currency_id AS VARCHAR(255)) as currency_id,
        CAST(vat_id AS INT) as vat_id,

        CAST(name AS VARCHAR(255)) AS name,
        CAST(code AS VARCHAR(255)) as code,
        CAST(description AS VARCHAR(255)) as description,
        CAST(description_type AS VARCHAR(255)) as description_type,
        CAST(measure AS VARCHAR(255)) as measure,
        CAST(sort AS VARCHAR(255)) as sort,
        CAST(price AS REAL) as opportunity,

        CASE 
            WHEN timestamp_x ~ '^\d{4}-\d{2}-\d{2}' THEN timestamp_x::TIMESTAMP
            ELSE NULL 
        END AS timestamp_x,
        CASE 
            WHEN date_create ~ '^\d{4}-\d{2}-\d{2}' THEN date_create::TIMESTAMP
            ELSE NULL 
        END AS date_create,

        CASE 
            WHEN active = 'Y' OR active = '1' THEN 1
            WHEN active = 'N' OR active = '0' OR active = '' THEN 0
            ELSE NULL 
        END AS is_active,
        CASE 
            WHEN vat_included = 'Y' OR vat_included = '1' THEN 1
            WHEN vat_included = 'N' OR vat_included = '0' OR vat_included = '' THEN 0
            ELSE NULL 
        END AS vat_included
    FROM {{ source('landing', 'test_db.crm_process_product') }}
    WHERE id IS NOT NULL
)

SELECT * FROM cleared