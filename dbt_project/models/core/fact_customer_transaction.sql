{{
    config(
        materialized='table',
        alias='fact_customer_transaction',
        unique_key='customer_transaction_id'
    )
}}

SELECT
    CustomerTransactionID AS customer_transaction_id, --business key/PK
    c.CustomerID  AS customer_id, --FK
    TransactionTypeID AS transaction_type_id,
    i.InvoiceID AS invoice_id, --FK
    PaymentMethodID AS payment_method_id,
    AmountExcludingTax AS amount_excluding_tax,
    TaxAmount AS tax_amount,
    TransactionAmount AS transaction_amount,
    OutstandingBalance AS outstanding_balance,
    FinalizationDate AS finalization_date,
    IsFinalized AS is_finalized
FROM {{ ref('CustomerTransaction') }} t
JOIN {{ ref('Invoice') }} i ON i.InvoiceID = t.InvoiceID
JOIN {{ ref('Customer') }} c ON c.CustomerID  = t.CustomerID 