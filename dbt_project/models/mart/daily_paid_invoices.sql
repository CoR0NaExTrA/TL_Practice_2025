{{
    config(
        materialized='table',
        alias='extended_invoice_metrics',
        unique_key='date_day'
    )
}}

WITH invoice_metrics AS (
    SELECT
        i.invoice_date AS date_day,
        i.customer_id,
        dc.customer_name,
        dc.customer_category_id,
        dc.buying_group_id,
        COUNT(DISTINCT i.invoice_id) AS total_invoices_count,
        COUNT(DISTINCT CASE WHEN ct.is_finalized = 1 THEN i.invoice_id END) AS paid_invoices_count,
        COUNT(DISTINCT CASE WHEN ct.is_finalized = 0 OR ct.is_finalized IS NULL THEN i.invoice_id END) AS pending_invoices_count,
        SUM(CASE WHEN ct.is_finalized = 1 THEN ct.transaction_amount ELSE 0 END) AS paid_amount,
        SUM(CASE WHEN ct.is_finalized = 0 OR ct.is_finalized IS NULL THEN ct.outstanding_balance ELSE 0 END) AS pending_amount
    FROM {{ ref('dim_invoice') }} i
    INNER JOIN {{ ref('dim_customer') }} dc ON i.customer_id = dc.customer_id
    LEFT JOIN {{ ref('fact_customer_transaction') }} ct ON i.invoice_id = ct.invoice_id
    GROUP BY 
        i.invoice_date,
        i.customer_id,
        dc.customer_name,
        dc.customer_category_id,
        dc.buying_group_id
)

SELECT
    date_day,
    customer_id,
    customer_name,
    customer_category_id,
    buying_group_id,
    total_invoices_count,
    paid_invoices_count,
    pending_invoices_count,
    paid_amount,
    pending_amount,
    SUM(paid_invoices_count) OVER (PARTITION BY customer_id ORDER BY date_day) AS cumulative_paid_invoices_customer,
    SUM(paid_invoices_count) OVER (ORDER BY date_day) AS cumulative_paid_invoices_total,
    SUM(pending_invoices_count) OVER (PARTITION BY customer_id ORDER BY date_day) AS cumulative_pending_invoices_customer,
    SUM(pending_invoices_count) OVER (ORDER BY date_day) AS cumulative_pending_invoices_total
FROM invoice_metrics