{{
    config(
        materialized='view',
        alias='stg_deal'
    )
}}

WITH cleared AS (
    SELECT
        CAST(id AS INT) AS deal_id,
        CAST(type_id AS VARCHAR(255)) as type_id,
        CAST(stage_id AS VARCHAR(255)) AS stage_id,
        CAST(currency_id AS VARCHAR(255)) as currency_id,
        CAST(lead_id AS INT) as lead_id,
        CAST(company_id AS VARCHAR(255)) as company_id,
        CAST(contact_id AS VARCHAR(255)) as contact_id,
        CAST(quote_id AS VARCHAR(255)) as quote_id,
        CAST(assigned_by_id AS INT) as assigned_by_id,
        CAST(created_by_id AS INT) as created_by_id,
        CAST(modify_by_id AS INT) as modify_by_id,
        CAST(location_id AS INT) as location_id,
        CAST(category_id AS INT) as category_id,
        CAST(stage_semantic_id AS VARCHAR(255)) as stage_semantic_id,
        CAST(source_id AS VARCHAR(255)) as source_id,
        CAST(originator_id AS VARCHAR(255)) as originator_id,
        CAST(origin_id AS VARCHAR(255)) as origin_id,
        CAST(moved_by_id AS INT) as moved_by_id,
        CAST(last_activity_by AS INT) as last_activity_by,

        CAST(title AS VARCHAR(255)) AS title,
        CAST(probability AS INT) as probability,
        CAST(opportunity AS REAL) as opportunity,
        CAST(tax_value AS REAL) as tax_value,
        CAST(comments AS VARCHAR(255)) as comments,
        CAST(additional_info AS VARCHAR(255)) as additional_info,
        CAST(source_description AS VARCHAR(255)) as source_description,

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
            WHEN begindate ~ '^\d{4}-\d{2}-\d{2}' THEN begindate::DATE
            ELSE NULL 
        END AS begin_date,
        CASE 
            WHEN closedate ~ '^\d{4}-\d{2}-\d{2}' THEN closedate::DATE
            ELSE NULL 
        END AS close_date,
        CASE 
            WHEN moved_time ~ '^\d{4}-\d{2}-\d{2}' THEN moved_time::TIMESTAMP
            ELSE NULL 
        END AS moved_time,
        
        CASE 
            WHEN is_manual_opportunity = 'Y' OR is_manual_opportunity = '1' THEN 1
            WHEN is_manual_opportunity = 'N' OR is_manual_opportunity = '0' OR is_manual_opportunity = '' THEN 0
            ELSE NULL 
        END AS is_manual_opportunity,
        CASE 
            WHEN is_new = 'Y' OR is_new = '1' THEN 1
            WHEN is_new = 'N' OR is_new = '0' OR is_new = '' THEN 0
            ELSE NULL 
        END AS is_new,
        CASE 
            WHEN is_recurring = 'Y' OR is_recurring = '1' THEN 1
            WHEN is_recurring = 'N' OR is_recurring = '0' OR is_recurring = '' THEN 0
            ELSE NULL 
        END AS is_recurring,
        CASE 
            WHEN is_return_customer = 'Y' OR is_return_customer = '1' THEN 1
            WHEN is_return_customer = 'N' OR is_return_customer = '0' OR is_return_customer = '' THEN 0
            ELSE NULL 
        END AS is_return_customer,
        CASE 
            WHEN is_repeated_approach = 'Y' OR is_repeated_approach = '1' THEN 1
            WHEN is_repeated_approach = 'N' OR is_repeated_approach = '0' OR is_repeated_approach = '' THEN 0
            ELSE NULL 
        END AS is_repeated_approach,
        CASE 
            WHEN opened = 'Y' OR opened = '1' THEN 1
            WHEN opened = 'N' OR opened = '0' OR opened = '' THEN 0
            ELSE NULL 
        END AS is_opened,
        CASE
            WHEN closed = 'Y' OR closed = '1' THEN 1
            WHEN closed = 'N' OR closed = '0' OR closed = '' THEN 0
            ELSE NULL 
        END AS is_closed
    FROM {{ source('landing', 'test_db.crm_process_deal') }}
    WHERE id IS NOT NULL
)

SELECT * FROM cleared