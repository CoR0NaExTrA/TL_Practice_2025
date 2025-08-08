{{
    config(
        materialized='view',
        alias='stg_customer_transaction'
    )
}}

WITH cleaned AS (
    SELECT
        TRY_CAST(CustomerTransactionID AS INT) AS CustomerTransactionID,

        TRY_CAST(CustomerID AS INT) AS CustomerID,
        TRY_CAST(TransactionTypeID AS INT) AS TransactionTypeID,
        TRY_CAST(InvoiceID AS INT) AS InvoiceID,
        TRY_CAST(PaymentMethodID AS INT) AS PaymentMethodID,

        TRY_CONVERT(DATE, TransactionDate, 120) AS TransactionDate,

        TRY_CAST(AmountExcludingTax AS MONEY) AS AmountExcludingTax,
        TRY_CAST(TaxAmount AS MONEY) AS TaxAmount, 
        TRY_CAST(TransactionAmount AS MONEY) AS TransactionAmount,
        TRY_CAST(OutstandingBalance AS MONEY) AS OutstandingBalance,

        TRY_CONVERT(DATE, FinalizationDate, 120) AS FinalizationDate,

        CASE 
        WHEN TRY_CAST(IsFinalized AS BIT) = 1 
            THEN 1 
            ELSE 0 
        END AS IsFinalized

    FROM {{ source('landing', 'CustomerTransaction') }}
)

SELECT * FROM cleaned
WHERE CustomerTransactionID IS NOT NULL 