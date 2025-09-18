{{
    config(
        materialized='view',
        alias='stg_company'
    )
}}

WITH cleared AS (
    SELECT
        CAST(id AS INT) AS company_id,
        CAST(lead_id AS INT) as lead_id,
        CAST(assigned_by_id AS INT) AS assigned_by_id,
        CAST(created_by_id AS INT) as created_by_id,
        CAST(modify_by_id AS INT) as modify_by_id,
        CAST(currency_id AS VARCHAR(255)) as currency_id,
        CAST(originator_id AS VARCHAR(255)) as originator_id,
        CAST(origin_id AS VARCHAR(255)) as origin_id,
        CAST(address_loc_addr_id AS INT) as address_loc_addr_id,
        CAST(reg_address_loc_addr_id AS INT) as reg_address_loc_addr_id,
        CAST(last_activity_by AS INT) as last_activity_by,

        CAST(company_type AS VARCHAR(255)) AS company_type,
        CAST(title AS VARCHAR(255)) as title,
        CAST(industry AS VARCHAR(255)) as industry,
        CAST(revenue AS MONEY) as revenue,
        CAST(employees AS VARCHAR(255)) as employees,
        CAST(origin_version AS VARCHAR(255)) as origin_version,

        CAST(address AS VARCHAR(255)) as address,
        CAST(address_2 AS VARCHAR(255)) as address_2,
        CAST(address_city AS VARCHAR(255)) as address_city,
        CAST(address_postal_code AS VARCHAR(255)) as address_postal_code,
        CAST(address_region AS VARCHAR(255)) as address_region,
        CAST(address_province AS VARCHAR(255)) as address_province,
        CAST(address_country AS VARCHAR(255)) as address_country,
        CAST(address_country_code AS VARCHAR(255)) as address_country_code,
        CAST(address_legal AS VARCHAR(255)) as address_legal,
        
        CAST(reg_address AS VARCHAR(255)) as reg_address,
        CAST(reg_address_2 AS VARCHAR(255)) as reg_address_2,
        CAST(reg_address_city AS VARCHAR(255)) as reg_address_city,
        CAST(reg_address_postal_code AS VARCHAR(255)) as reg_address_postal_code,
        CAST(reg_address_region AS VARCHAR(255)) as reg_address_region,
        CAST(reg_address_province AS VARCHAR(255)) as reg_address_province,
        CAST(reg_address_country AS VARCHAR(255)) as reg_address_country,
        CAST(reg_address_country_code AS VARCHAR(255)) as reg_address_country_code,

        CAST(utm_source AS VARCHAR(255)) as utm_source,
        CAST(utm_medium AS VARCHAR(255)) as utm_medium,
        CAST(utm_campaign AS VARCHAR(255)) as utm_campaign,
        CAST(utm_content AS VARCHAR(255)) as utm_content,
        CAST(utm_term AS VARCHAR(255)) as utm_term,

        CASE 
            WHEN date_create ~ '^\d{4}-\d{2}-\d{2}' THEN date_modify::TIMESTAMP
            ELSE NULL 
        END AS date_create,
        CASE 
            WHEN date_modify ~ '^\d{4}-\d{2}-\d{2}' THEN date_modify::TIMESTAMP
            ELSE NULL 
        END AS date_modify,
        CASE 
            WHEN last_activity_time ~ '^\d{4}-\d{2}-\d{2}' THEN last_activity_time::TIMESTAMP
            ELSE NULL 
        END AS last_activity_time,

        CASE 
            WHEN has_phone = 'Y' OR has_phone = '1' THEN 1
            WHEN has_phone = 'N' OR has_phone = '0' OR has_phone = '' THEN 0
            ELSE NULL 
        END AS has_phone,
        CASE 
            WHEN has_email = 'Y' OR has_email = '1' THEN 1
            WHEN has_email = 'N' OR has_email = '0' OR has_email = '' THEN 0
            ELSE NULL 
        END AS has_email,
        CASE 
            WHEN has_imol = 'Y' OR has_imol = '1' THEN 1
            WHEN has_imol = 'N' OR has_imol = '0' OR has_imol = '' THEN 0
            ELSE NULL 
        END AS has_imol,
        CASE 
            WHEN opened = 'Y' OR opened = '1' THEN 1
            WHEN opened = 'N' OR opened = '0' OR opened = '' THEN 0
            ELSE NULL 
        END AS is_opened,
        CASE 
            WHEN is_my_company = 'Y' OR is_my_company = '1' THEN 1
            WHEN is_my_company = 'N' OR is_my_company = '0' OR is_my_company = '' THEN 0
            ELSE NULL 
        END AS is_my_company
    FROM {{ source('landing', 'test_db.crm_process_company') }}
    WHERE id IS NOT NULL
)

SELECT * FROM cleared