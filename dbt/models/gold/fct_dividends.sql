select
    dividend_id,
    ticker,
    ex_dividend_date,
    declaration_date,
    record_date,
    pay_date,
    cash_amount,
    currency,
    frequency,
    distribution_type,
    historical_adjustment_factor
from {{ ref('stg_dividends') }}
