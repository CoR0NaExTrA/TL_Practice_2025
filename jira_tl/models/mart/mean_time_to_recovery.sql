{{ config (
    materialized='table'
    ) 
}}

WITH base AS (

    SELECT
        incident_id,
        issue_id,
        priority_name,
        region,
        service_code,
        unavailability_min,
        -- Переводим даты в MSK (+3 часа от UTC)
        DATEADD(hour, 3, CAST(date_begin as datetime2)) AS date_begin_msk,
        DATEADD(hour, 3, CAST(date_end   as datetime2)) AS date_end_msk
    FROM {{ ref('fact_incident') }}
    WHERE
        -- фильтр по приоритетам
        priority_name IN ('Critical', 'Normal', N'Критический', 'Minor', 'Blocker')
        AND region IN ('RU', 'Asia', 'RU+Asia')
        AND service_code IN 
        (
            'BookingForm', 
            'Payments', 
            'WebPMS', 
            'Accounts', 
            'PMS', 
            'Ext', 
            'CM', 
            'FB', 
            'EndpointsSecure'
        )
        AND coalesce(unavailability_min, 0) > 0
        AND date_begin >= '2025-01-01'
),

expanded AS (
    SELECT incident_id, issue_id, priority_name,
           CASE WHEN region = 'RU+Asia' THEN 'RU' ELSE region END AS region,
           service_code, unavailability_min,
           date_begin_msk, date_end_msk
    FROM base
    UNION ALL
    SELECT incident_id, issue_id, priority_name,
           CASE WHEN region = 'RU+Asia' THEN 'Asia' ELSE region END AS region,
           service_code, unavailability_min,
           date_begin_msk, date_end_msk
    FROM base
    WHERE region = 'RU+Asia'
),

classified AS (
    SELECT
        incident_id,
        issue_id,
        priority_name,
        region,
        CASE
            WHEN service_code IN ('FB','BookingForm') THEN '1. Booking Form'
            WHEN service_code =  'Payments' THEN '2. Payments'
            WHEN service_code =  'Ext' THEN '3. Extranet'
            WHEN service_code =  'WebPMS' THEN '4. WepPms'
            WHEN service_code =  'Accounts' THEN '5. Accounts'
            WHEN service_code =  'PMS' THEN '6. PMS'
            WHEN service_code =  'CM' THEN '7. Channel Manager'
            WHEN service_code =  'EndpointsSecure' THEN N'8. Secure.*'
        END AS product,
        unavailability_min,
        date_begin_msk,
        date_end_msk,
        CASE
            WHEN priority_name IN ('Critical', N'Критический', 'Blocker')
                THEN 1  -- значимый
            WHEN priority_name IN ('Normal', 'Minor')
                THEN 0  -- незначимый
        END AS is_significant
    FROM expanded
),

aggregated AS (
    SELECT
        product,
        region,
        CAST(date_begin_msk as date) AS incident_date,
        COUNT(DISTINCT incident_id) AS total_incidents_count,
        COUNT(DISTINCT CASE WHEN is_significant = 1 THEN incident_id END) AS significant_incidents_count,
        COUNT(DISTINCT CASE WHEN is_significant = 0 THEN incident_id END) AS nonsignificant_incidents_count,
        SUM(CASE WHEN is_significant = 1 THEN unavailability_min ELSE 0 END) AS significant_downtime_min,
        AVG(CASE WHEN is_significant = 1 THEN unavailability_min END) AS mean_time_to_recovery_min
    FROM classified
    GROUP BY product, region, CAST(date_begin_msk as date)
)

SELECT  
    product,
    region,
    incident_date,
    total_incidents_count,
    significant_incidents_count,
    nonsignificant_incidents_count,
    significant_downtime_min,
    mean_time_to_recovery_min
FROM aggregated