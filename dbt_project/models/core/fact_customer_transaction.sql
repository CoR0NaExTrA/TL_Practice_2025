{{
    config(
        materialized='view',
        alias='dim_order'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['CustomerTransactionID']) }} AS transaction_key, --PK/surrogate key
    CustomerTransactionID AS customer_transaction_id, --business key
    c.customer_key AS customer_key, --FK
    TransactionTypeID AS transaction_type_id,
    i.invoice_key AS invoice_key, --FK
    payment_method_id,
    d.date_key AS date_key, --FK
    AmountExcludingTax AS amount_excluding_tax,
    TaxAmount AS tax_amount,
    TransactionAmount AS transaction_amount,
    OutstandingBalance AS outstanding_balance,
    FinalizationDate AS finalization_date,
    IsFinalized AS is_finalized
FROM {{ ref('Invoice') }} t
JOIN {{ ref('dim_invoice') }} i ON i.invoice_key = t.invoice_key
JOIN {{ ref('dim_customer') }} c ON c.customer_key = t.customer_key
JOIN {{ ref('dim_date') }} d ON d.date_key = t.date_key