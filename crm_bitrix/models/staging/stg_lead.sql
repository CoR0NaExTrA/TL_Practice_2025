{{
    config(
        materialized='view',
        alias='stg_lead'
    )
}}

WITH cleared AS (
    SELECT
        CAST(id AS INT) AS lead_id,
        CAST(honorific AS VARCHAR(255)) AS honorific,
        CAST(company_id AS INT) as company_id,
        CAST(contact_id AS INT) as contact_id,
        CAST(source_id AS VARCHAR(255)) as source_id,
        CAST(status_id AS VARCHAR(255)) as status_id,
        CAST(currency_id AS VARCHAR(255)) as currency_id,
        CAST(assigned_by_id AS VARCHAR(255)) as assigned_by_id,
        CAST(created_by_id AS VARCHAR(255)) as created_by_id,
        CAST(modify_by_id AS VARCHAR(255)) as modify_by_id,
        CAST(status_semantic_id AS VARCHAR(255)) as status_semantic_id,
        CAST(originator_id AS VARCHAR(255)) as originator_id,
        CAST(origin_id AS VARCHAR(255)) as origin_id,
        CAST(moved_by_id AS INT) as moved_by_id,
        CAST(address_loc_addr_id AS INT) as address_loc_addr_id,
        CAST(last_activity_by AS INT) as last_activity_by,

        CAST(title AS VARCHAR(255)) AS title,
        CAST(name AS VARCHAR(255)) AS name,
        CAST(second_name AS VARCHAR(255)) as second_name,
        CAST(last_name AS VARCHAR(255)) as last_name,
        CAST(company_title AS VARCHAR(255)) AS company_title,
        CAST(source_description AS VARCHAR(255)) as source_description,
        CAST(status_description AS VARCHAR(255)) as status_description,
        CAST(comments AS VARCHAR(255)) as comments,
        CAST(post AS VARCHAR(255)) as post,
        CAST(opportunity AS REAL) as opportunity,

        CAST(address AS VARCHAR(255)) as address,
        CAST(address_2 AS VARCHAR(255)) as address_2,
        CAST(address_city AS VARCHAR(255)) as address_city,
        CAST(address_postal_code AS VARCHAR(255)) as address_postal_code,
        CAST(address_region AS VARCHAR(255)) as address_region,
        CAST(address_province AS VARCHAR(255)) as address_province,
        CAST(address_country AS VARCHAR(255)) as address_country,
        CAST(address_country_code AS VARCHAR(255)) as address_country_code,

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
            WHEN date_closed ~ '^\d{4}-\d{2}-\d{2}' THEN date_closed::TIMESTAMP
            ELSE NULL 
        END AS date_closed,
        CASE 
            WHEN last_activity_time ~ '^\d{4}-\d{2}-\d{2}' THEN last_activity_time::TIMESTAMP
            ELSE NULL 
        END AS last_activity_time,
        CASE 
            WHEN birthdate ~ '^\d{4}-\d{2}-\d{2}' THEN birthdate::DATE
            ELSE NULL 
        END AS birthdate,
        CASE 
            WHEN moved_time ~ '^\d{4}-\d{2}-\d{2}' THEN moved_time::TIMESTAMP
            ELSE NULL 
        END AS moved_time,

        CASE 
            WHEN is_return_customer = 'Y' OR is_return_customer = '1' THEN 1
            WHEN is_return_customer = 'N' OR is_return_customer = '0' OR is_return_customer = '' THEN 0
            ELSE NULL 
        END AS is_return_customer,
        CASE 
            WHEN is_manual_opportunity = 'Y' OR is_manual_opportunity = '1' THEN 1
            WHEN is_manual_opportunity = 'N' OR is_manual_opportunity = '0' OR is_manual_opportunity = '' THEN 0
            ELSE NULL 
        END AS is_manual_opportunity,
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
        END AS is_opened
    FROM {{ source('landing', 'test_db.crm_process_lead') }}
    WHERE id IS NOT NULL
)

SELECT * FROM cleared