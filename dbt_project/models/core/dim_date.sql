{{
    config(
        materialized='table',
        alias='dim_date',
        unique_key='date_id'
    )
}}

WITH date_range AS (
    SELECT 
        DATEADD(DAY, [number], '2010-01-01') AS [date]
    FROM 
        master.dbo.spt_values
    WHERE 
        type = 'P' 
        AND [number] BETWEEN 0 AND (365 * 20 + 6)
),

enriched_dates AS (
    SELECT
        [date],
        CONVERT(INT, FORMAT([date], 'yyyyMMdd')) AS date_id,
        YEAR([date]) AS [year],
        DATEPART(QUARTER, [date]) AS [quarter],
        MONTH([date]) AS [month],
        DAY([date]) AS [day],
        DATEPART(WEEKDAY, [date]) AS day_of_week,
        
        DATENAME(MONTH, [date]) AS month_name,
        DATENAME(WEEKDAY, [date]) AS day_name,
        
        CASE WHEN DATEPART(WEEKDAY, [date]) IN (6, 7) THEN 1 ELSE 0 END AS is_weekend,
        CASE WHEN [date] = EOMONTH([date]) THEN 1 ELSE 0 END AS is_last_day_of_month,
        
        DATEPART(ISO_WEEK, [date]) AS week_of_year
    FROM date_range
)

SELECT
    date_id,
    [date],
    [year],
    [quarter],
    [month],
    [day],
    day_of_week,
    month_name,
    day_name,
    is_weekend,
    is_last_day_of_month,
    week_of_year,
    GETDATE() AS dbt_loaded_at
FROM enriched_dates