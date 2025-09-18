{{
    config(
        materialized='view',
        alias='stg_field'
    )
}}

WITH cleared AS (
    SELECT
        --Индентификаторы
        index as field_id,
        CAST(table_name AS VARCHAR(255)) AS table_name,

        --Основная информация
        CAST(type AS VARCHAR(50)) as field_type,
        CAST(title AS VARCHAR(255)) as field_title,
        CAST(statustype AS VARCHAR(50)) as status_type,

        --Флаги
        CASE 
            WHEN isrequired = 'true' OR isrequired = '1' THEN 1
            WHEN isrequired = 'false' OR isrequired = '0' OR isrequired = '' THEN 0
            ELSE NULL 
        END AS is_required,
        CASE 
            WHEN isreadonly = 'true' OR isreadonly = '1' THEN 1
            WHEN isreadonly = 'false' OR isreadonly = '0' OR isreadonly = '' THEN 0
            ELSE NULL 
        END AS is_readonly,
        CASE 
            WHEN isimmutable = 'true' OR isimmutable = '1' THEN 1
            WHEN isimmutable = 'false' OR isimmutable = '0' OR isimmutable = '' THEN 0
            ELSE NULL 
        END AS is_immutable,
        CASE 
            WHEN ismultiple = 'true' OR ismultiple = '1' THEN 1
            WHEN ismultiple = 'false' OR ismultiple = '0' OR ismultiple = '' THEN 0
            ELSE NULL 
        END AS is_multiple,
        CASE 
            WHEN isdynamic = 'true' OR isdynamic = '1' THEN 1
            WHEN isdynamic = 'false' OR isdynamic = '0' OR isdynamic = '' THEN 0
            ELSE NULL 
        END AS is_dynamic,
        CASE 
            WHEN isdeprecated = 'true' OR isdeprecated = '1' THEN 1
            WHEN isdeprecated = 'false' OR isdeprecated = '0' OR isdeprecated = '' THEN 0
            ELSE NULL 
        END AS is_deprecated,

        --Настройки и конфигурация
        settings,

        --Системная информация
        CASE 
            WHEN _airflow_emitted_at ~ '^\d{4}-\d{2}-\d{2}' THEN _airflow_emitted_at::TIMESTAMP
            ELSE NULL 
        END AS airflow_emitted_at
    FROM {{ source('landing', 'test_db.crm_process_field') }}
    WHERE 
    index IS NOT NULL 
    AND index != ''
    AND table_name IS NOT NULL 
    AND table_name != ''
    AND type IS NOT NULL 
    AND type != ''
    AND title IS NOT NULL 
    AND title != ''
)

SELECT * FROM cleared