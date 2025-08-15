{{
    config(
        materialized='table',
        alias='daily_paid_invoices',
        unique_key='date_day'
    )
}}

WITH paid_invoices AS (
    SELECT
        i.invoice_date AS date_day,
        COUNT(DISTINCT i.invoice_id) AS paid_invoices_count
    FROM {{ ref('dim_invoice') }} i
    JOIN {{ ref('fact_customer_transaction') }} ct ON i.invoice_id = ct.invoice_id
    WHERE ct.is_finalized = 1 
    GROUP BY i.invoice_date
)

SELECT
    date_day,
    paid_invoices_count,
    SUM(paid_invoices_count) OVER (ORDER BY date_day) AS cumulative_paid_invoices
FROM paid_invoices
ORDER BY date_day