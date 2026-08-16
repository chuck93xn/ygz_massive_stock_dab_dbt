-- SCD2 dimension: keeps every historical version, not just the current one.
-- Joining to this table by ticker alone can fan out fact rows once a ticker
-- has more than one version - see _gold__models.yml for the join convention.
select
    ticker,
    name,
    primary_exchange,
    sic_code,
    type,
    active,
    currency_name,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('dim_ticker_snapshot') }}
