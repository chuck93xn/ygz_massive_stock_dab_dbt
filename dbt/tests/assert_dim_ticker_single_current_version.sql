-- SCD2 invariant: a ticker must never have more than one is_current row.
-- A generic unique() test on `ticker` doesn't fit here (multiple historical
-- rows per ticker are expected) - this is the constraint that actually
-- matters.
select
    ticker,
    count(*) as current_version_count
from {{ ref('dim_ticker') }}
where is_current
group by ticker
having count(*) > 1
