# openjobdata-tracker

A personal job-search pipeline built on the public [openjobdata](https://openjobdata.com) dataset.
Streams job-posting data from a public Hugging Face bucket straight into Neon Postgres —
nothing is stored locally — keeping only roles I'm targeting (GTM Engineer, Sales Ops,
Revenue Ops). The end goal: point an agent (Claude Cowork) at the Neon database and have
it surface/apply to new matches.

Built as a learning project for SQL + Python data pipelines.

## How it works

**The contract: the `jobs` table holds every title-matched job — all dates, open and
closed alike; the `target_role_jobs` view is the open-only "apply to these" lens.**
An agent pointed at the view can treat every row as appliable; the underlying table
keeps full history for trend analysis. When a delta reports a stored job as closed,
the upsert flips its status in place and the view stops showing it.

1. **`etl/backfill.py`** — one-time initial load. Streams each base parquet shard from the
   bucket into memory, keeps title matches, and upserts them into Neon. The trimmed
   companies lookup (`COMPANY_COLUMNS`) is loaded first so the `jobs.company_id`
   foreign key always resolves.
2. **`etl/sync.py`** — daily delta sync. Pulls the `changes/{date}.parquet` files, applies
   the same title filter, and upserts with a bulk `INSERT ... ON CONFLICT DO UPDATE`.
3. **`queries/target_roles.sql`** — SQL view + queries over the stored jobs, for practicing
   SQL and finding jobs to apply to.
4. **`scripts/daily_scan.py`** — run on a schedule (cron, or handed to an agent); reports
   matches that are new since the last run.

Only jobs matching the target titles are ever written to the database, which keeps
storage comfortably inside Neon's free tier (the full unfiltered dataset is ~1.7GB;
the filtered set is a few MB).

## Setup

### 1. Database

1. Create a free Neon project at https://neon.tech
2. Copy the connection string (looks like
   `postgresql://user:password@ep-xxxx.region.aws.neon.tech/dbname?sslmode=require`)
3. Put it in a `.env` file (see `.env.example`)

### 2. Python environment

```bash
python -m venv venv
source venv/bin/activate       # on Windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env           # then fill in your DATABASE_URL
```

### 3. Create tables

```bash
python -m db.init_db
```

### 4. Initial load (streams from bucket, no local files)

```bash
python -m etl.backfill
```

### 5. Daily sync (going forward)

```bash
python -m etl.sync --variant minimal
```

Pulls only the last day's `changes/` file by default — cheap to run daily.
Use `--backfill` to re-sync every delta since `EARLIEST_KNOWN_DELTA`, or
`--start-date`/`--end-date` for an explicit range.

### 6. Find target-role jobs

```bash
psql "$DATABASE_URL" -f queries/target_roles.sql
```

or:

```bash
python -m scripts.daily_scan
```

## Changing what gets stored

Target titles live in one place: `TARGET_TITLE_PATTERNS` / `EXCLUDE_TITLE_PATTERNS`
in `etl/sync.py`. The view in `queries/target_roles.sql` uses matching ILIKE patterns —
update both together. After widening the patterns, re-run `python -m etl.backfill`
to pick up historical jobs that now match.

## Data notes

- The `minimal` variant has job-level fields only (title, department, country,
  remote flag, workplace type, dates, apply URL). It does **not** include salary,
  seniority, or city/region-level location — those columns don't exist upstream.
- The `full` variant adds `entire_json` / `job_model_json` (nested JSON per job).
  `etl/fetch_full_for_matches.py` is meant to backfill descriptions for matched jobs
  from it, but currently expects a flat `description` column that doesn't exist —
  it needs updating to parse the JSON columns before use.
- The source `country` field is inconsistent (sometimes a city like "Salt Lake City").
- Companies are stored trimmed to: id, name, unique_id, career_url, region, country,
  industry, linkedin_url.

## Project structure

```
openjobdata-tracker/
├── db/
│   ├── models.py         SQLAlchemy models (Company, Job)
│   └── init_db.py        Creates tables from models
├── etl/
│   ├── backfill.py       One-time streaming load from the bucket (filtered)
│   ├── sync.py           Daily delta sync + shared filter/upsert helpers
│   └── fetch_full_for_matches.py   Optional: backfill descriptions (needs JSON fix)
├── queries/
│   └── target_roles.sql  SQL view + queries for target job titles
├── scripts/
│   └── daily_scan.py     Runs the target-role query, reports what's new
├── requirements.txt
├── .env.example
└── README.md
```

## Data source

All data comes from the public [openjobdata](https://openjobdata.com) dataset, hosted on
Hugging Face at `hf://buckets/Invicto69/Jobs-Dataset-bucket`. No API key required — it's a
public bucket. See https://openjobdata.com/documentation for the upstream docs.
