{{ config(
    materialized='incremental',
    unique_key='event_row_id',
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT
        event_row_id,
        issue_id,
        issue_key,
        priority_name,
        region,
        service_name,
        service_code,
        unavailability_min,
        date_begin,
        date_end,
        created_at,
        updated_at
    FROM {{ ref('stg_jira_issue_event') }}

    {% if is_incremental() %}
      WHERE updated_at > (SELECT coalesce(max(updated_at), '1970-01-01') FROM {{ this }})
    {% endif %}
)

SELECT
    event_row_id AS incident_id,
    issue_id,
    issue_key,
    priority_name,
    CASE WHEN region = 'RU+Asia' THEN 'Asia' ELSE region END AS region,
    service_name,
    service_code,
    unavailability_min,
    date_begin,
    date_end,
    created_at,
    updated_at,
    current_timestamp AS load_timestamp
FROM source
WHERE region IN ('Asia', 'RU+Asia')
