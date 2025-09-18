{{
    config(
        materialized='view',
        alias='stg_user'
    )
}}

WITH cleared AS (
    SELECT
        CAST(id AS INT) AS user_id,
        CAST(xml_id AS VARCHAR(255)) AS xml_id,
        CAST(modified_by AS INT) as modified_by,
        CAST(created_by AS INT) as created_by,
        CAST(catalog_id AS INT) as catalog_id,
        CAST(section_id AS INT) as section_id,
        CAST(currency_id AS VARCHAR(255)) as currency_id,
        CAST(vat_id AS INT) as vat_id,

        CAST(name AS VARCHAR(255)) AS name,
        CAST(last_name AS VARCHAR(255)) as last_name,
        CAST(second_name AS VARCHAR(255)) as second_name,
        CAST(title AS VARCHAR(255)) as title,
        CAST(email AS VARCHAR(255)) as email,
        CAST(personal_gender AS VARCHAR(255)) as personal_gender,
        CAST(personal_photo AS VARCHAR(255)) as personal_photo,
        CAST(personal_gender AS VARCHAR(255)) as personal_gender,
        CAST(personal_gender AS VARCHAR(255)) as personal_gender,
        CAST(personal_gender AS VARCHAR(255)) as personal_gender,
        CAST(personal_gender AS VARCHAR(255)) as personal_gender,
        CAST(personal_gender AS VARCHAR(255)) as personal_gender,
        CAST(work_phone AS VARCHAR(255)) as work_phone,
        CAST(work_position AS VARCHAR(255)) as work_position,
        CAST(work_country AS VARCHAR(255)) as personal_gender,
        CAST(user_type AS VARCHAR(255)) as user_type,

        CASE 
            WHEN last_login ~ '^\d{4}-\d{2}-\d{2}' THEN last_login::TIMESTAMP
            ELSE NULL 
        END AS last_login,
        CASE 
            WHEN date_register ~ '^\d{4}-\d{2}-\d{2}' THEN date_register::TIMESTAMP
            ELSE NULL 
        END AS date_register,

        CASE 
            WHEN active = 'true' OR active = '1' THEN 1
            WHEN active = 'false' OR active = '0' OR active = '' THEN 0
            ELSE NULL 
        END AS is_active,
        CASE 
            WHEN is_online = 'Y' OR is_online = '1' THEN 1
            WHEN is_online = 'N' OR is_online = '0' OR is_online = '' THEN 0
            ELSE NULL 
        END AS is_online
    FROM {{ source('landing', 'test_db.crm_process_product') }}
    WHERE id IS NOT NULL
)

SELECT * FROM cleared

"time_zone"
"is_online"
"time_zone_offset"
"timestamp_x"
"last_activity_date"
"personal_gender"
"personal_profession"
"personal_www"
"personal_birthday"
"personal_photo"
"personal_icq"
"personal_phone"
"personal_fax"
"personal_mobile"
"personal_pager"
"personal_street"
"personal_city"
"personal_state"
"personal_zip"
"personal_country"
"personal_mailbox"
"personal_notes"
"work_phone"
"work_company"
"work_position"
"work_department"
"work_www"
"work_fax"
"work_pager"
"work_street"
"work_mailbox"
"work_city"
"work_state"
"work_zip"
"work_country"
"work_profile"
"work_notes"
"uf_employment_date"
"uf_department"
"user_type"