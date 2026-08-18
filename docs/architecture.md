# Architecture

A stock-watchlist pipeline built with **Databricks Asset Bundles (DAB)** and
**dbt Core**, ingesting real market data from [massive.com](https://massive.com/)
for a 10-ticker watchlist (`AAPL, MSFT, AMZN, JPM, JNJ, TSLA, XOM, KO, DIS, BA`)
and modeling it into an analytics-ready warehouse.

For the reasoning behind specific decisions and the bugs found along the way,
see [`engineering-log.md`](./engineering-log.md). This document covers what
the system looks like today and why it's shaped this way.

## Table of contents

- [Medallion layers](#medallion-layers)
- [Ingestion: `MassiveClient`](#ingestion-massiveclient)
- [Data model](#data-model)
- [Gold business rules](#gold-business-rules)
- [Three environments, one workspace](#three-environments-one-workspace)

## Medallion layers

| Layer | Storage | Owner | Content |
| --- | --- | --- | --- |
| **Landing** | Raw JSON in a Unity Catalog Volume, append-only | Python | Raw API responses, partitioned by date, kept for traceability |
| **Bronze** | Delta tables, schema fixed but unclean | Python/PySpark (DAB job) | JSON parsed into structured columns + `_ingested_at`/`_source_file` audit fields |
| **Silver** | Delta tables, cleaned & typed | dbt Core | Deduped, renamed, typed, dbt tests |
| **Gold** | Delta tables, analytics-facing | dbt Core | Derived metrics: returns, moving averages, volume anomalies, aggregated by ticker + date |

Five data types flow through all four layers daily: daily bars, ticker
overview, splits, dividends, and news (with sentiment).

## Ingestion: `MassiveClient`

`src/ingestion/landing/client.py` wraps massive.com's REST API behind one
class, one public method per endpoint, two shared private helpers
(`_request` for auth + retry, `_paginate` for cursor pagination):

| Method | Endpoint | Pagination |
| --- | --- | --- |
| `get_daily_bars(ticker, start, end)` | `GET /v2/aggs/ticker/{ticker}/range/...` | Follows `next_url` to completion |
| `get_ticker_overview(ticker)` | `GET /v3/reference/tickers/{ticker}` | **None** — `results` is a single dict, not a list |
| `get_splits(ticker)` | `GET /stocks/v1/splits` | Follows `next_url` to completion |
| `get_dividends(ticker)` | `GET /stocks/v1/dividends` | Follows `next_url` to completion |
| `get_news(ticker)` | `GET /v2/reference/news` | Follows `next_url`, but **capped at 5 records** |

**Two different pagination behaviors, on purpose.** `_paginate` only knows
how to follow `next_url` until it runs out — it has no concept of "enough."
For daily bars/splits/dividends that's correct: the caller wants the whole
requested range. For news, the business requirement caps results at 5 per
ticker, so `get_news` wraps `_paginate` with its own counter and returns
early — passing `limit=5` straight through to `_paginate` only constrains
page size, not total results (confirmed by testing: it kept paginating past
5, to 19).

**Rate limiting**: massive.com's free tier allows 5 requests/minute. The
client paces requests 13 seconds apart (60/5 rounded up) and, on a 429 with
no usable rate-limit header, waits a flat 65 seconds rather than a short
exponential backoff. See [`engineering-log.md`](./engineering-log.md) for how
that number was actually found — it took several rounds of real failures to
get there.

## Data model

Classification rule: **things that recur/accumulate over time are facts;
relatively static descriptions of an entity are dimensions.** Splits,
dividends, and news read like "information" but are event records — one row
per event — so they're modeled as facts, not dimensions.

```
Dimensions:
  dim_ticker        -- SCD2 (slow-changing: company name, exchange, SIC code, ...)
  dim_date          -- date spine (dbt_utils.date_spine)
  dim_publisher      -- deduped from news article publisher metadata
  dim_industry       -- sic_code -> sic_description lookup (snowflaked out of dim_ticker)

Core facts (direct pass-through from Silver):
  fct_daily_bars              -- ticker + trade_date, OHLCV
  fct_ticker_daily_metrics    -- ticker + snapshot_date, market_cap and other fast-changing fields
  fct_splits / fct_dividends  -- event-grained
  fct_news_sentiment          -- article_id + ticker (bridge), inner-joined to article metadata

Derived facts (Gold business rules, see below):
  fct_daily_returns      -- daily return %, significant-move flag
  fct_moving_averages    -- 5d/20d/50d moving averages, trend classification
  fct_volume_anomalies   -- volume ratio vs. 20d average, spike flag
```

### `dim_ticker`: why SCD2, and why it's split from `fct_ticker_daily_metrics`

Ticker Overview returns two kinds of fields, handled differently:

- **Slow-changing** (`name`, `primary_exchange`, `sic_code`, `type`, `active`,
  `currency_name`) go into `dim_ticker`, tracked via a **dbt snapshot**
  (`dbt/snapshots/dim_ticker_snapshot.sql`, `strategy: check`,
  `unique_key: ticker`). dbt's snapshot mechanism handles versioning natively
  — no hand-written `row_number()`/`lag()` merge logic. `dim_ticker.sql`
  exposes `valid_from`/`valid_to`/`is_current` on top of the snapshot's
  standard `dbt_valid_from`/`dbt_valid_to` columns.
- **Fast-changing** (`market_cap`, share counts) go into their own fact table,
  `fct_ticker_daily_metrics` (grain: ticker + snapshot_date). These change
  daily along with the stock price — forcing them into the SCD2 dimension
  would open a new `dim_ticker` version every day for every ticker, which
  defeats the point of a dimension being relatively stable.

The watchlist is 10 large, stable mega-caps, so `dim_ticker`'s SCD2 versions
rarely actually change in practice — but SCD2 was chosen from the start
rather than shipping SCD1 and refactoring later if a field ever changes.

A singular test, `assert_dim_ticker_single_current_version.sql`, asserts
at most one `is_current = true` row per ticker — the real invariant an SCD2
table needs to hold (plain `unique(ticker)` would be wrong, since a ticker
can legitimately have multiple historical rows).

### Other dimensions

- **`dim_date`**: generated via `dbt_utils.date_spine`, range computed
  dynamically from `stg_daily_bars` (`min(trade_date)` to `max(trade_date) +
  30 days` — the 30-day buffer keeps future fact rows from failing to join
  once the underlying data grows past the spine's original end date).
  `is_trading_day` is currently simplified to "not a weekend" (US market
  holidays aren't excluded yet).
- **`dim_publisher`**: deduped from the news payload's nested `publisher`
  object — there's no dedicated publisher-list endpoint.
- **`dim_industry`**: `sic_code` → `sic_description`, snowflaked out of
  `dim_ticker` so the SCD2 dimension only carries the code, not the
  description (the SIC code's meaning doesn't change; which code a ticker is
  classified under does, and that's already tracked by `dim_ticker`'s SCD2).

## Gold business rules

Three thresholds were left open by the original requirements and had to be
decided explicitly:

| Rule | Threshold | Implementation |
| --- | --- | --- |
| Significant move | ±3% daily return | `fct_daily_returns.is_significant_move` |
| Trend | 5d MA vs. 20d MA, ±1% band for "sideways" | `fct_moving_averages.trend` |
| Volume spike | ≥2x the trailing 20-day average | `fct_volume_anomalies.is_volume_spike` |

Implementation details worth calling out:

- The 1% "sideways" band isn't one of the three confirmed thresholds — it
  was added during implementation because a pure moving-average crossover
  only produces "up" or "down," and the requirement explicitly wanted a
  three-way trend classification.
- `fct_volume_anomalies`'s 20-day average volume window **deliberately
  excludes the current day** (`rows between 20 preceding and 1 preceding`),
  unlike the moving-average windows elsewhere which include it. Including
  the spike day in its own baseline would inflate the baseline and dilute
  the ratio, understating the very spike being measured.
- Verified against real data, not just passing tests: the 5 largest single-day
  moves (MSFT +15.5%, AMZN +15.3%, TSLA -14.5%, ...) were all correctly
  flagged; trend classification produced a real mix of all three states
  (1000 uptrend / 785 downtrend / 725 sideways rows) rather than degenerating
  into one bucket; volume spikes fired on 71 real rows with plausible
  magnitudes (DIS 5.27x, MSFT 4.90x, AMZN 4.88x), not zero and not everywhere.

## Three environments, one workspace

`dev` / `test` / `prod` share a single Databricks workspace and differ only
by Unity Catalog catalog (`ygz_massive_stock_dev/test/prod`) — no separate
workspaces, no service principal. That tradeoff (and the ones behind it) is
covered in [`engineering-log.md`](./engineering-log.md#three-environment-isolation-is-structural-not-a-convention),
along with why `test`/`prod` get their data by copying `dev`'s Landing
Volume rather than calling massive.com independently.
