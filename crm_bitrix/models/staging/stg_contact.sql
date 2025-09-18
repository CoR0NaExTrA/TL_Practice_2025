{{
    config(
        materialized='view',
        alias='stg_contact'
    )
}}

WITH cleared AS (
    SELECT
        CAST(id AS INT) AS contact_id,
        CAST(honorific AS VARCHAR(255)) AS honorific,
        CAST(lead_id AS INT) as lead_id,
        CAST(type_id AS INT) AS type_id,
        CAST(source_id AS INT) as source_id,
        CAST(company_id AS INT) as company_id,
        CAST(assigned_by_id AS VARCHAR(255)) as assigned_by_id,
        CAST(created_by_id AS VARCHAR(255)) as created_by_id,
        CAST(modify_by_id AS VARCHAR(255)) as modify_by_id,
        CAST(originator_id AS INT) as originator_id,
        CAST(face_id AS INT) as face_id,
        CAST(address_loc_addr_id AS INT) as address_loc_addr_id,
        CAST(last_activity_by AS INT) as last_activity_by,

        CAST(name AS VARCHAR(255)) AS name,
        CAST(second_name AS VARCHAR(255)) as second_name,
        CAST(last_name AS VARCHAR(255)) as last_name,
        CAST(source_description AS VARCHAR(255)) as source_description,
        CAST(comments AS VARCHAR(255)) as comments,
        CAST(post AS VARCHAR(255)) as post,
        CAST(origin_version AS VARCHAR(255)) as origin_version,

        CAST(address AS VARCHAR(255)) as address,
        CAST(address_2 AS VARCHAR(255)) as address_2,
        CAST(address_city AS VARCHAR(255)) as address_city,
        CAST(address_postal_code AS VARCHAR(255)) as address_postal_code,
        CAST(address_region AS VARCHAR(255)) as address_region,
        CAST(address_province AS VARCHAR(255)) as address_province,
        CAST(address_country AS VARCHAR(255)) as address_country,

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
            WHEN birthdate ~ '^\d{4}-\d{2}-\d{2}' THEN birthdate::DATE
            ELSE NULL 
        END AS birthdate,

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
            WHEN export = 'Y' OR export = '1' THEN 1
            WHEN export = 'N' OR export = '0' OR export = '' THEN 0
            ELSE NULL 
        END AS export,
        CASE 
            WHEN opened = 'Y' OR opened = '1' THEN 1
            WHEN opened = 'N' OR opened = '0' OR opened = '' THEN 0
            ELSE NULL 
        END AS is_opened
    FROM {{ source('landing', 'test_db.crm_process_contact') }}
    WHERE id IS NOT NULL
)

SELECT * FROM cleared