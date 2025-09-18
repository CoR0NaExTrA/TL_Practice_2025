{{
    config(
        materialized='view',
        alias='stg_lead_product'
    )
}}

WITH cleared AS (
    SELECT
        CAST(id AS INT) AS lead_product_id,
        CAST(owner_id AS VARCHAR(255)) AS owner_id,
        CAST(product_id AS INT) as product_id,
        CAST(discount_type_id AS INT) as discount_type_id,
        CAST(xml_id AS VARCHAR(255)) as xml_id,
        CAST(store_id AS VARCHAR(255)) as store_id,
        CAST(reserve_id AS VARCHAR(255)) as reserve_id,

        CAST(owner_type AS VARCHAR(255)) AS owner_type,
        CAST(product_name AS VARCHAR(255)) AS product_name,
        CAST(original_product_name AS VARCHAR(255)) as original_product_name,
        CAST(product_description AS VARCHAR(255)) as product_description,
        CAST(price AS INT) as price,
        CAST(price_exclusive AS INT) as price_exclusive,
        CAST(price_netto AS INT) as price_netto,
        CAST(price_brutto AS INT) as price_brutto,
        CAST(price_account AS REAL) as price_account,
        CAST(quantity AS INT) as quantity,
        CAST(discount_rate AS INT) as discount_rate,
        CAST(discount_sum AS INT) as discount_sum,
        CAST(tax_rate AS INT) as tax_rate,
        CAST(measure_code AS INT) as measure_code,
        CAST(measure_name AS VARCHAR(255)) as measure_name,
        CAST(sort AS INT) as sort,
        CAST(type AS INT) as type,
        CAST(reserve_quantity AS INT) as reserve_quantity,

        CASE 
            WHEN date_reserve_end ~ '^\d{4}-\d{2}-\d{2}' THEN date_reserve_end::TIMESTAMP
            ELSE NULL 
        END AS date_reserve_end,

        CASE 
            WHEN tax_included = 'Y' OR tax_included = '1' THEN 1
            WHEN tax_included = 'N' OR tax_included = '0' OR tax_included = '' THEN 0
            ELSE NULL 
        END AS tax_included,
        CASE 
            WHEN customized = 'Y' OR customized = '1' THEN 1
            WHEN customized = 'N' OR customized = '0' OR customized = '' THEN 0
            ELSE NULL 
        END AS is_customized
    FROM {{ source('landing', 'test_db.crm_process_lead_product_map') }}
    WHERE id IS NOT NULL
)

SELECT * FROM cleared