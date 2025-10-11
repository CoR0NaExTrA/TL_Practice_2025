{{ config (
    materialized='incremental',
    unique_key='event_row_id'
    ) 
}}


WITH source AS (
    SELECT
        CAST(event_row_id AS varchar(255)) AS event_row_id,
        CAST(issue_id AS int) AS issue_id,
        CAST(issue_key AS varchar(255)) AS issue_key,
        CAST(priority_name AS varchar(100)) AS priority_name,
        CAST(region AS varchar(100)) AS region,
        CAST([service_name] AS varchar(255)) AS [service_name],
        CAST(service_code AS varchar(50)) AS service_code,
        CAST(unavailability_min AS bigint) AS unavailability_min,
        CAST(date_begin AS datetime2) AS date_begin,
        CAST(date_end AS datetime2) AS date_end,
        CAST(created_at AS datetime2) AS created_at,
        CAST(updated_at AS datetime2) AS updated_at,
        CURRENT_TIMESTAMP AS loaded_at
    FROM {{ source('jira_tl', 'issue_event_test') }}

)

SELECT
    event_row_id,
    issue_id,
    issue_key,
    priority_name,
    region,
    [service_name],
    service_code,
    unavailability_min,
    date_begin,
    date_end,
    created_at,
    updated_at,
    loaded_at
FROM source