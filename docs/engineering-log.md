# Engineering log

Key decisions, tradeoffs, and real bugs from building this pipeline, organized
by topic rather than by timeline. This is a condensed, English rewrite of a
much longer set of Chinese process notes kept locally during development
(`plan/records/`, gitignored) — kept short here on purpose, but without
smoothing over the specific numbers, error messages, and root causes, since
those are what actually hold up under interview follow-up questions.

## Table of contents

- [Three-environment isolation is structural, not a convention](#three-environment-isolation-is-structural-not-a-convention)
- [CI/CD: four iterations, each driven by a real failure](#cicd-four-iterations-each-driven-by-a-real-failure)
- [Incremental loading: the first design failed, visibly](#incremental-loading-the-first-design-failed-visibly)
- [Three production bugs](#three-production-bugs)
- [Rate-limit investigation](#rate-limit-investigation)
- [Cost-aware scheduling](#cost-aware-scheduling)
- [Pausing and resuming](#pausing-and-resuming)

## Three-environment isolation is structural, not a convention

Early on, `prod` was a fully empty catalog — deploys only pushed job
*definitions*, nothing ever ran there. The first fix was to let `prod` call
massive.com independently, which was rejected: it would fight `dev` for the
same 5-requests/minute quota for no benefit. The design that stuck is
**promotion**: `dev` is the only environment that calls the real API;
`test`/`prod` get their data by copying `dev`'s already-fetched Landing
Volume and running Bronze/Silver/Gold on top of the copy.

The first implementation of that idea was rejected too, for a sharper reason.
It added a `promote_to_prod_job` but left `ingestion_daily_job`/
`ingestion_reference_job` deployed to `prod` as well, just `PAUSED`. The
counter-argument: `pause_status: PAUSED` only blocks the *cron* trigger — it
does nothing to stop someone from manually running the job (CLI or "Run now"
in the UI). As long as those jobs are deployed to `prod` at all, there's a
live path to accidentally calling the real API and writing into the prod
catalog, directly contradicting the "prod never calls the API" decision. A
job-description comment warning against doing that was floated first and
rejected — a docstring isn't a guardrail.

Before redesigning, whether DAB supports "deploy this resource everywhere
except target X" was checked against the actual docs and CLI issue tracker
rather than assumed: the [overrides documentation](https://docs.databricks.com/dev-tools/bundles/overrides)
only describes merging/overriding fields on a shared resource, not excluding
it; [databricks/cli#2872](https://github.com/databricks/cli/issues/2872) asked
for exactly this and was closed as a duplicate of
[#2878](https://github.com/databricks/cli/issues/2878), which is still open
with no supported workaround. So the fix isn't "shared definition + exclude,"
it's the other direction DAB actually supports: **a resource defined inside a
target's own `resources:` block only ever deploys to that target.**

```
ingestion_daily_job / ingestion_reference_job  -> databricks.yml targets.dev.resources.jobs
promote_from_dev_job                           -> targets.test.resources.jobs AND targets.prod.resources.jobs (duplicated)
dbt_job                                        -> shared resources/jobs.yml (all three targets)
```

Verified structurally, not just "no validation error":

```
$ databricks bundle validate -t dev  -o json | jq '.resources.jobs | keys'
["dbt_job", "ingestion_daily_job", "ingestion_reference_job"]
$ databricks bundle validate -t test -o json | jq '.resources.jobs | keys'
["dbt_job", "promote_from_dev_job"]
$ databricks bundle validate -t prod -o json | jq '.resources.jobs | keys'
["dbt_job", "promote_from_dev_job"]
```

`ingestion_daily_job`/`ingestion_reference_job` simply don't exist in the
resolved config for `test`/`prod` — there's no job to accidentally run.

A latent bug turned up while doing this: the `python_wheel_task`s never
actually passed `${var.catalog}` through to the Python code (only
`dbt_task` has a native `catalog:` field), so every real job run had been
silently defaulting to the dev catalog regardless of target. Fixed by adding
a `parameters: ["${var.catalog}"]` to each task and reading it from
`sys.argv` in `Settings.from_job_argv()`.

### `dbt_job` is the one deliberate exception, and it can't be fully closed

Unlike `ingestion_daily_job`/`ingestion_reference_job`/`promote_from_dev_job`,
`dbt_job` is intentionally the one job left in the shared
`resources/jobs.yml` — pure SQL transforms, no external side effects, safe to
define once for all three targets (see the comment there). `dev`/`test` each
override its `schedule:` to their own cadence; `prod` doesn't override it at
all, so it inherits the shared default (`pause_status: PAUSED`) — meaning
prod's "this never auto-runs" guarantee depends on that shared default
staying `PAUSED`, not on a structural absence of a schedule the way
`promote_from_dev_job` has. That's exactly the kind of "convention, not
structure" gap this section's title argues against.

Tried closing it: overriding `schedule: ~` under `targets.prod.resources.jobs.dbt_job`,
then a full redefinition of the job under the same key with no `schedule:`
field at all. Neither worked — `bundle validate -t prod -o json` kept
resolving the full inherited schedule dict either way. DAB's target-level
merge only replaces fields the override actually sets; it has no mechanism to
delete a field the shared base defines, no matter how much of the rest of the
job gets redefined in the override. The only way to give prod's `dbt_job` a
genuinely schedule-less definition would be pulling it out of the shared file
and writing a third full copy per target — reversing the DRY decision this
job was specifically kept around to preserve, for one job whose failure mode
(prod's dbt build running once on a schedule instead of never) is low-stakes
compared to the ingestion jobs it's structurally protecting `test`/`prod`
from. Decided to leave it as a documented, known gap rather than duplicate
the job definition.

The two jobs that *are* structurally manual-only — `ingestion_backfill_job`
in dev, and `promote_from_dev_job` in **prod specifically** (test's
`promote_from_dev_job` has its own real, currently-paused weekly schedule,
unlike prod's) — have `(manual)` appended to their `name:` so they're
visually distinguishable from "has a schedule, currently paused" jobs in the
Databricks UI job list. Prod's `dbt_job` deliberately does *not* get that
suffix, since it isn't actually true for it.

## CI/CD: four iterations, each driven by a real failure

The workflow file layout changed four times. None of the changes were
speculative — each one followed a concrete problem hit while actually using
the pipeline.

**1. Three files → two.** The original `test.yml`/`deploy-dev.yml`/
`deploy-prod.yml` triggered on the same events but had no dependency between
them — `test` could fail and `deploy` would run anyway. Merged into
`ci-dev.yml` (PR-triggered) / `cd-prod.yml` (push-to-main-triggered), with
`deploy-*` jobs gated by `needs: test`.

**2. Two files → four**, after the three-environment restructuring made
"deploy dev/test/prod" too coarse to express in two files. Each file mapped
to exactly one trigger: `ci-validate.yml` (push, non-main → validate dev
only), `ci-dev.yml` (PR → deploy dev), `cd-test.yml` (push main → deploy
test), `cd-prod.yml` (`workflow_dispatch` only → deploy prod).

**3. Four files → two**, after a real false alarm. GitHub Actions' sidebar
sorts workflows by their `name:` field alphabetically, not by pipeline order
— with four files, "CD - deploy prod" sorted above "CI - validate dev,"
which briefly looked like prod had been deployed automatically (it turned
out to be a stale run, not a new trigger, but the confusion was real).
Splitting into more files hadn't reduced cognitive load, it had just added
more names to track. Merged back into `ci-dev.yml` / `cd-test-prod.yml`,
using job-level `if: github.event_name == '...'` to distinguish behaviors
within a single file.

**4. Two files → three**, after a second real trap surfaced. With `test`/
`deploy-test`/`deploy-prod` all in `cd-test-prod.yml`, every push-triggered
run (i.e. every merge to main) showed `deploy-prod` as a "skipped" job. That
looked clickable via "Re-run jobs" — but re-running a run keeps its original
`github.event_name`, so the `if: github.event_name == 'workflow_dispatch'`
guard stayed false and the re-run silently did nothing. The fix pulls
`deploy-prod` into its own file, `cd-prod.yml`, whose *only* trigger is
`workflow_dispatch` — it structurally never appears inside an automatic run,
so there's no skipped job to be tempted by. `cd-test.yml` keeps the
push-triggered `test`/`deploy-test` jobs. Naming (`CD 1 - deploy test (auto
on merge to main)` / `CD 2 - deploy prod (manual only)`) keeps the sidebar
ordering from causing the same confusion as iteration 2 — numeric prefixes
pin `CD 1` above `CD 2` regardless of alphabetical sort, and the names spell
out auto vs. manual instead of requiring the reader to infer it.

The throughline: **workflow topology kept changing because production usage
kept surfacing UX traps that validation and code review didn't catch** — a
docs-only fix was rejected twice in this project specifically because it
doesn't remove the trap, it just adds a sentence next to it.

## Incremental loading: the first design failed, visibly

Bronze originally reloaded every table from scratch on every run. The first
incremental design assumed all 5 data sources behave the same way: Landing
files partitioned by `date=YYYY-MM-DD`, Bronze reads only partitions newer
than a watermark derived from `max(_ingested_at)`.

That assumption is false for 3 of the 5 sources. `splits`/`dividends` have no
date parameter at all — every call returns the full historical event list;
`ticker_overview` has no concept of history, every call is a full current
snapshot. Only `daily_bars` (a 5-day rolling window) and `news` (a 7-day
rolling window) are genuinely incremental at the API level.

Tested directly rather than assumed correct: loaded Bronze from empty
(baseline: 2510/10/10/605/36/245 rows across the 6 tables), then re-ran
**without clearing anything**. Every table's row count exactly doubled
(5020/20/20/1210/72/490). Root cause: partition pruning assumes "a new
partition only ever brings new records," which holds for `daily_bars` but
not for the full-replay sources — a "new" `date=` partition full of already-seen
splits/dividends/overview data still got appended as if it were new.

The fix moved deduplication from *read time* to *write time*: read Landing in
full every run (small enough that this isn't a performance problem), and
before writing to Bronze, `left_anti`-join against existing primary keys in
the target table to drop rows already present (`_exclude_existing_keys()` in
`src/ingestion/bronze/loader.py`). `left_anti` was chosen over `MERGE INTO`
because it's plain DataFrame API — testable without a real Delta table, and
consistent with the rest of the module. Re-verified with the same
clear-then-rerun-then-rerun-again test: second rerun now leaves row counts
**unchanged**, the exact contrast to the first design's failure.

## Three production bugs

### `dbfs:` scheme in `promote_from_dev_job`

After `test`'s catalog was created and `promote_from_dev_job` ran for real,
`load_bronze_promoted` failed with `[PATH_NOT_FOUND]`. The real Landing
Volume showed an extra `_data/` subfolder that shouldn't exist.

Root cause, confirmed with the actual strings rather than guessed:
`dbutils.fs.ls()` returns `FileInfo.path` values prefixed with `dbfs:`, even
for `/Volumes/...` paths, but the code's own path constants
(`DEV_LANDING_VOLUME_PATH`, `settings.landing_volume_path`) are scheme-less.
The relative-path computation sliced by `len(source_path)`, which was 5
characters short (the length of `"dbfs:"`), so every "relative path" kept a
stray 5-character tail of `source_path` glued to the front — and the last 5
characters of `raw_massive_data` happen to be `_data`:

```python
>>> len('/Volumes/ygz_massive_stock_dev/landing/raw_massive_data')
55
>>> 'dbfs:/Volumes/.../raw_massive_data/daily_bars/...'[55:]
'_data/daily_bars/...'
```

The copy task itself reported success — `dbutils.fs.cp` doesn't care whether
the destination path is the one you meant, it just copies. The failure only
surfaced downstream, in the job that read from the (wrong) correct path.

Fixed with a `_strip_dbfs_scheme()` helper applied consistently at both the
listing and copy-entry points. The existing test fakes had modeled `dbutils`
returning *clean*, scheme-less paths — which is exactly why the tests hadn't
caught this; the fakes were more well-behaved than the real API. Updated the
fakes to return `dbfs:`-prefixed paths (matching reality) and added a
regression test asserting the exact destination path. Verified the new test
actually catches the regression, not just that it passes: `git stash`ed the
fix, reran the tests, confirmed all three failed with `_data` visible in the
output, then restored the fix and confirmed green.

### Wheel dependency path breaks after moving jobs into `databricks.yml`

Structurally moving `ingestion_daily_job` etc. from `resources/jobs.yml` into
`databricks.yml` (see the environment-isolation section above) broke
`bundle deploy` with:

```
Error: no files match pattern: ../dist/*.whl
  at resources.jobs.ingestion_daily_job.environments[0].spec.dependencies[0]
  in databricks.yml:91:21
```

Relative paths inside a DAB resource definition resolve relative to **the
YAML file that declares them**, not the bundle root. `../dist/*.whl` was
correct when the job lived in `resources/jobs.yml` (one directory below
root); moved into `databricks.yml` (at the root), the same string now points
outside the bundle entirely. `bundle validate` doesn't catch this — it
doesn't build the wheel — so this only surfaced on a real `deploy`. Fixed by
dropping the `../`; re-verified by actually running `python -m build --wheel`
locally and confirming the output path matched.

### `generate_schema_name` doubling (`silver_silver`, `gold_gold`)

The first real `dbt build` put tables in `silver_silver`/`gold_gold` instead
of `silver`/`gold`. dbt's default `generate_schema_name` macro concatenates a
model's custom `+schema:` config with the target schema when they're set to
the same value — which they were (`+schema: "silver"` and target schema
`"silver"`). Present since the initial scaffold, only exposed once a real
`dbt build` ran against the warehouse. Fixed with a custom
`dbt/macros/generate_schema_name.sql` that uses the custom schema as-is
instead of concatenating.

## Rate-limit investigation

The daily ingestion job kept failing on massive.com's news endpoint with
429s. The fix took several rounds of testing hypotheses against real runs,
not one lucky guess:

| Attempt | Change | Result |
| --- | --- | --- |
| 1 | 1-second delay between requests | Still 429 (page 2 of a news pagination call), ~104s total |
| 2 | 3-second delay | Failed at the **same point**, ~103s total — barely moved despite tripling the delay |
| 3 | Split the single ingestion job into daily/weekly jobs to cut per-run call count | Still 429, and failed *earlier* than before splitting |
| 4 | Checked massive.com's docs directly: free tier is **5 requests/minute** | — |
| 5 | 13-second delay (60/5, +1s margin); on 429 with no usable header, a flat 65-second wait | **Success** — full job completed in 7m53s |

Attempts 1–2 ruled out "just needs a little more spacing" — failure timing
barely changing across a 3x delay change pointed at a time-window-based
limit, not simple burst throttling. Attempt 3 ruled out "total call volume is
the problem" — regardless of how calls were grouped across jobs, spacing
them at 1–3 seconds (60/20 calls per minute) still blew past a 5/minute
limit almost immediately; splitting the job didn't change the per-second
rate at all. Only reading the actual vendor documentation (attempt 4)
produced the number that made the fix work.

The job split from attempt 3 was kept anyway, on its own merits — of the 5
data types, only `daily_bars`/`news` genuinely need daily refresh;
`ticker_overview`/`splits`/`dividends` are slow-changing and the vendor API
has no incremental concept for them regardless. That produced
`ingestion_daily_job` (daily bars + news) and `ingestion_reference_job`
(overview/splits/dividends, weekly) — which then needed a second fix: both
jobs' cron expressions initially landed on the same Sunday timestamp, and
since the rate limit is per API key (not per job), two jobs starting at once
could jointly exceed it even with correct internal pacing. Offset the
reference job's schedule by 12 hours to fix.

## Cost-aware scheduling

All three environments' job schedules stayed `PAUSED` from the start of the
project, specifically to avoid triggering real cost before there was a
reason to. Unpausing was a deliberate, environment-tiered decision rather
than "turn everything on":

- **dev** (the only environment that calls the real API) runs on its full
  intended cadence: `ingestion_daily_job` daily, `ingestion_reference_job`
  weekly, `dbt_job` daily.
- **test** runs weekly (`promote_from_dev_job` + `dbt_job`, offset 30 minutes
  apart) — a validation environment doesn't need daily freshness.
- **prod** stays fully manual, no schedule at all — consistent with prod
  deploys already requiring a deliberate `workflow_dispatch` click.

Rough cost estimate from Azure serverless job compute pricing
(~$0.70-0.95/DBU-hour, billed by the second) against each job's real observed
runtime: **~$40-50/year** for dev's full daily/weekly cadence, **~$5-10/year**
for test's weekly cadence. Order-of-magnitude, not a real invoice — but
enough to make "just leave it paused forever" a clearly unnecessary tradeoff.

One implementation snag: `dbt_job` is a single shared definition in
`resources/jobs.yml`, but the three environments now needed three different
schedules. DAB's docs state that target-level `resources:` overrides merge
with the shared definition, but don't spell out whether that merge is
field-level (only the fields you specify change) or whole-block (the entire
`schedule:` object gets replaced, silently dropping anything not repeated).
Rather than assume, this was tested directly with `bundle validate -t dev -o
json`: overriding only `schedule:` in the target block left `tasks` intact
and only replaced the schedule fields, confirming field-level merge and
avoiding a full duplicate `dbt_job` definition per target.

## Pausing and resuming

The schedules above ran for a while, then got paused again (`dev`'s 3 jobs
and `test`'s 2, back to `pause_status: PAUSED`) once the recurring cost
became real instead of theoretical, with a plan to resume roughly a month
later. Three of the five data sources behave completely differently under a
month-long gap, which drove the resume design:

- **`daily_bars`** — `land_daily_data()` only ever looks at a trailing
  5-day window (`ingestion/landing/job.py`), so a month-long pause leaves a
  permanent hole unless something re-lands the missing range.
- **`news`** — `get_news(days_back, limit)`'s `limit` is a hard cap on the
  *total* returned, not a page size, so widening `days_back` after a gap
  doesn't recover anything: it still returns the same "most recent 5"
  articles, just risks them being staler if the default 7-day window would
  otherwise have found fewer. Nothing to backfill here — resuming with the
  normal defaults is already correct.
- **`ticker_overview`/`splits`/`dividends`** — the vendor always returns
  full current state/history regardless of how long it's been since the
  last call, so these have no gap to fill either.

Only `daily_bars` needed a real fix: `ingestion_backfill_job`, a
manual-trigger-only DAB job (`land_daily_bars_backfill()` → `load_bronze`,
deliberately no `schedule:` block at all, not just `PAUSED` — same reasoning
as `promote_from_dev_job`) that re-lands ~365 days per ticker. Bronze's
natural-key anti-join makes re-landing an overlapping range safe to run
without any special-casing.

Resume checklist:

1. Flip the 5 `pause_status: PAUSED` schedules back to `UNPAUSED` in
   `databricks.yml`, `bundle deploy -t dev` / `-t test`.
2. `databricks bundle run ingestion_backfill_job -t dev` once — backfills
   `daily_bars` and loads it into Bronze.
3. Nothing else needs manual backfilling — `ingestion_daily_job`/
   `ingestion_reference_job` running on their restored schedule (or
   triggered once manually to not wait for the next cron tick) is already
   correct for `news`/`ticker_overview`/`splits`/`dividends`.
4. `databricks bundle run dbt_job -t dev` once the Bronze backfill has
   landed, so `fct_moving_averages`/`fct_volume_anomalies` (window functions
   over consecutive trade dates) recompute across the now-complete history
   instead of treating the gap month as missing data.
5. `databricks bundle run promote_from_dev_job -t test` (and the prod
   equivalent, manually, as always) if test/prod need the refreshed data
   before their own schedule picks it up.

For the current state of all 8 jobs across all three environments (which are
paused-but-scheduled vs. structurally manual-only), see
[`architecture.md#jobs`](./architecture.md#jobs).
