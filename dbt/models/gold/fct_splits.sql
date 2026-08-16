select
    split_id,
    ticker,
    execution_date,
    split_from,
    split_to,
    adjustment_type,
    historical_adjustment_factor
from {{ ref('stg_splits') }}
