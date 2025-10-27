{{ config(
    materialized='table'
) }}

WITH base AS (
    SELECT * FROM {{ ref('fact_incident_ru') }}
    UNION ALL
    SELECT * FROM {{ ref('fact_incident_asia') }}
),

filtered AS (
    SELECT
        incident_id,
        issue_id,
        priority_name,
        region,
        service_code,
        unavailability_min,
        DATEADD(hour, 3, CAST(date_begin as datetime2)) AS date_begin_msk,
        DATEADD(hour, 3, CAST(date_end as datetime2))   AS date_end_msk
    FROM base
    WHERE
        priority_name IN ('Critical', 'Normal', N'Критический', 'Minor', 'Blocker')
        AND service_code IN (
            'BookingForm', 'Payments', 'WebPMS', 'Accounts',
            'PMS', 'Ext', 'CM', 'FB', 'EndpointsSecure'
        )
        AND coalesce(unavailability_min, 0) > 0
        AND date_begin >= '2025-01-01'
),

classified AS (
    SELECT
        incident_id,
        issue_id,
        priority_name,
        region,
        CASE
            WHEN service_code IN ('FB','BookingForm') THEN '1. Booking Form'
            WHEN service_code = 'Payments' THEN '2. Payments'
            WHEN service_code = 'Ext' THEN '3. Extranet'
            WHEN service_code = 'WebPMS' THEN '4. WebPMS'
            WHEN service_code = 'Accounts' THEN '5. Accounts'
            WHEN service_code = 'PMS' THEN '6. PMS'
            WHEN service_code = 'CM' THEN '7. Channel Manager'
            WHEN service_code = 'EndpointsSecure' THEN N'8. Secure.*'
        END AS product,
        unavailability_min,
        date_begin_msk,
        date_end_msk,
        CASE
            WHEN priority_name IN ('Critical', N'Критический', 'Blocker') THEN 1
            ELSE 0
        END AS is_significant
    FROM filtered
),

aggregated AS (
    SELECT
        product,
        region,
        CAST(date_begin_msk AS date) AS incident_date,
        COUNT(DISTINCT incident_id) AS total_incidents_count,
        COUNT(DISTINCT CASE WHEN is_significant = 1 THEN incident_id END) AS significant_incidents_count,
        SUM(CASE WHEN is_significant = 1 THEN unavailability_min ELSE 0 END) AS significant_downtime_min,
        AVG(CASE WHEN is_significant = 1 THEN unavailability_min END) AS mean_time_to_recovery_min
    FROM classified
    GROUP BY product, region, CAST(date_begin_msk AS date)
)

SELECT * FROM aggregated
