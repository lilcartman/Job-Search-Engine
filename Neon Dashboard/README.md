# Cowork pipeline console

A small read-only web dashboard for the Neon Postgres database behind the job-search pipeline. One Python file serves both the API and the page. It introspects whatever tables exist (`jobs`, `sync_log`, anything else), so no schema is hardcoded — if the pipeline adds a table, it just shows up.

## What you get

- A sync ticker across the top showing the latest `sync_log` row (skipped gracefully if the table doesn't exist)
- A sidebar listing every table with live row counts
- A data grid with text search, click-to-sort columns, and pagination (50 rows/page)
- URLs render as links, booleans as ✓/✗, nulls as ∅

It is read-only by design: the app never writes to the database.

## Setup

1. Install dependencies:

   ```
   pip install -r requirements.txt
   ```

2. Copy `.env.example` to `.env` and paste your Neon connection string (Neon console → your project → Connect → connection string). Keep `sslmode=require` on the end.

3. Run it:

   ```
   uvicorn app:app --reload
   ```

4. Open http://localhost:8000

## Deploying it later (optional)

The app runs anywhere Python runs. Render and Railway both have free tiers:

- Push this folder to a GitHub repo
- On Render: New → Web Service → connect the repo, build command `pip install -r requirements.txt`, start command `uvicorn app:app --host 0.0.0.0 --port $PORT`
- Set `DATABASE_URL` as an environment variable in the service settings — never commit your `.env`

Note: deployed as-is, the dashboard is public to anyone with the URL. Fine for demoing as a portfolio piece with non-sensitive data; add auth before putting anything private behind it.

## Notes

- Connection pooling is built in (1–4 connections) since Neon cold connections are slow.
- Table and column names in queries are validated against `information_schema`, and search values are parameterized, so there's no SQL injection surface.
- To make a table load first by default, it prefers one named `jobs`; otherwise it picks the first table alphabetically.
