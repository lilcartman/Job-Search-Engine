"""
Downloads daily changes (deltas) from the openjobdata Hugging Face bucket and
upserts them directly into Postgres. Companies are synced first so the
`jobs.company_id` foreign key never points at a missing row, then job deltas
are streamed in with a bulk INSERT ... ON CONFLICT DO UPDATE.

Usage:
    # Backfill everything from the earliest available delta up to today
    python -m etl.sync --variant minimal --backfill

    # Just sync the last day's changes (what you'd run on a daily cron / agent)
    python -m etl.sync --variant minimal

    # Explicit date range
    python -m etl.sync --variant minimal --start-date 2026-05-28 --end-date 2026-06-01
"""

import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
from dotenv import load_dotenv
from huggingface_hub import HfFileSystem
from sqlalchemy import bindparam, create_engine, MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import OperationalError

load_dotenv()


def _utcnow_naive() -> datetime:
    """Current UTC time as a naive datetime, comparable with DB timestamps."""
    return datetime.now(timezone.utc).replace(tzinfo=None)

BUCKET_URI = "hf://buckets/Invicto69/Jobs-Dataset-bucket"
DATABASE_URL = os.getenv("DATABASE_URL")

# The earliest delta file the bucket is known to have. Update if you find
# an earlier one available, or pass --start-date explicitly.
EARLIEST_KNOWN_DELTA = "2026-05-28"

# Only these columns from companies.parquet are kept — matches db.models.Company.
COMPANY_COLUMNS = [
    "id", "name", "unique_id", "career_url", "region", "country",
    "industry", "linkedin_url",
]

# This pipeline is INDUSTRY-driven, not title-driven: every job posted by a
# company in these industries is stored (compared against companies.industry,
# lowercase). Title slicing happens downstream in the view.
TARGET_INDUSTRIES = [
    "real estate",
    "commercial real estate",
]

# Development-track title patterns. They do NOT gate storage — they drive the
# target_role_jobs view slice and pick which rows get city enrichment from
# the heavy full variant. Mirror the ILIKE list in queries/target_roles.sql.
TARGET_TITLE_PATTERNS = [
    "development",
    "acquisition",
    "asset manag",
    "investment",
    "real estate",
]


def log_sync_run(engine, kind: str, rows: int, started: float,
                 status: str = "ok", detail: str = None):
    """
    Appends one row to sync_log — the pipeline's flight recorder. The
    dashboard ticker displays the latest row. Never let logging kill a run
    that otherwise succeeded (e.g. sync_log table not created yet).
    """
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO sync_log "
                    "(kind, rows_synced, duration_seconds, status, detail) "
                    "VALUES (:k, :r, :d, :s, :t)"
                ),
                {"k": kind, "r": rows, "d": int(time.time() - started),
                 "s": status, "t": detail},
            )
    except Exception as exc:
        print(f"(sync_log write failed: {exc})")


def target_company_ids(engine) -> set:
    """
    Ids of companies whose industry matches TARGET_INDUSTRIES. Reads from the
    companies table, so call it after the companies sync has run.
    """
    stmt = text(
        "SELECT id FROM companies WHERE lower(industry) IN :inds"
    ).bindparams(bindparam("inds", expanding=True))
    with engine.connect() as conn:
        return set(conn.execute(stmt, {"inds": TARGET_INDUSTRIES}).scalars())


def filter_target_jobs(df: pd.DataFrame, company_ids: set) -> pd.DataFrame:
    """
    Keeps every row posted by a target-industry company, PLUS any row whose
    title itself mentions real estate (catches corporate real-estate roles
    at banks/retailers/etc. that aren't tagged as real-estate companies).
    All dates, open and closed alike — the jobs table is the full history;
    the target_role_jobs view is the open-only development-track lens.
    """
    by_industry = df["company_id"].isin(company_ids)
    by_title = df["title"].str.contains("real estate", case=False, na=False)
    return df[by_industry | by_title]


def _extract_city(raw) -> str | None:
    """
    Pulls location.city out of one job_model_json value. The blob is
    double-encoded (a JSON string whose payload is itself JSON), so it may
    need two decode passes.
    """
    if raw is None:
        return None
    try:
        d = json.loads(raw)
        if isinstance(d, str):
            d = json.loads(d)
        return (d.get("location") or {}).get("city")
    except (ValueError, TypeError):
        return None


def add_city_from_full(fs, full_remote_path: str, jobs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a 'city' column, populated only for rows whose title matches
    TARGET_TITLE_PATTERNS (the development-track slice the view shows).
    City lives in job_model_json in the FULL variant, and those files are
    huge — every row carries its description inside the JSON — so we fetch
    the blob just for the sliced ids via the pyarrow row-group filter.
    Everything else keeps city NULL.
    """
    jobs_df = jobs_df.copy()
    jobs_df["city"] = None
    include = "|".join(TARGET_TITLE_PATTERNS)
    mask = jobs_df["title"].str.contains(include, case=False, na=False)
    wanted = jobs_df.loc[mask, "unique_id"].tolist()
    if not wanted:
        return jobs_df

    try:
        full_df = pd.read_parquet(
            full_remote_path,
            filesystem=fs,
            columns=["id", "job_model_json"],
            filters=[("id", "in", wanted)],
        )
    except FileNotFoundError:
        return jobs_df  # no full-variant counterpart; keep city as NULL

    city_by_id = {
        row.id: _extract_city(row.job_model_json)
        for row in full_df.itertuples()
    }
    jobs_df["city"] = jobs_df["unique_id"].map(city_by_id)
    return jobs_df


def prepare_jobs_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames raw job-shard columns to match db.models.Job. The source's own
    'id' field is a string dedupe key, not our integer primary key, so it
    maps to unique_id; is_remote/close_time are just renamed.
    """
    return df.rename(columns={
        "id": "unique_id",
        "is_remote": "remote",
        "close_time": "closed_at",
    })


def upsert_dataframe(engine, table: Table, df: pd.DataFrame, conflict_col: str, retries: int = 2):
    """Bulk INSERT ... ON CONFLICT DO UPDATE for a dataframe against a table."""
    if df.empty:
        return 0

    records = df.astype(object).where(pd.notnull(df), None).to_dict(orient="records")

    # Neon suspends idle compute; a connection can die while a parquet shard
    # downloads. Each attempt is a single transaction, so retrying after a
    # dropped connection is safe — the upsert is idempotent.
    for attempt in range(retries + 1):
        try:
            with engine.begin() as conn:
                stmt = insert(table).values(records)
                update_cols = {
                    c.name: stmt.excluded[c.name]
                    for c in table.columns
                    if c.name != conflict_col
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=[conflict_col], set_=update_cols
                )
                conn.execute(stmt)
            return len(records)
        except OperationalError:
            if attempt == retries:
                raise
            engine.dispose()
            time.sleep(5)


def sync_database_with_bucket(
    variant: str = "minimal", start_date: str = None, end_date: str = None
):
    """
    Downloads daily changes (deltas) from the Hugging Face bucket and upserts
    them directly into a PostgreSQL database. This avoids RAM exhaustion by
    writing directly to the database rather than concatenating large files
    in memory.
    """
    if not DATABASE_URL:
        raise SystemExit(
            "DATABASE_URL is not set. Copy .env.example to .env and fill it in."
        )

    started = time.time()
    fs = HfFileSystem()  # token not required for public buckets
    # pool_pre_ping revalidates pooled connections Neon may have killed
    # while the script was busy downloading shards.
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    if start_date is None:
        # Self-healing default: resume from the day of the last successful
        # sync (tracked via synced_at in Neon), so missed days are caught up
        # automatically. Capped at 14 days to bound a worst-case gap.
        with engine.connect() as conn:
            last = conn.execute(text("SELECT MAX(synced_at) FROM jobs")).scalar()
        if last is not None:
            start = max(last - timedelta(days=1), _utcnow_naive() - timedelta(days=14))
        else:
            start = _utcnow_naive() - timedelta(days=1)
        start_date = start.strftime("%Y-%m-%d")
    if end_date is None:
        end_date = _utcnow_naive().strftime("%Y-%m-%d")

    target_dates = pd.date_range(start=start_date, end=end_date, freq="D")
    metadata = MetaData()
    companies_table = Table("companies", metadata, autoload_with=engine)
    jobs_table = Table("jobs", metadata, autoload_with=engine)

    # Companies lookup is small — sync it in full each run so FKs resolve.
    companies_remote_path = f"{BUCKET_URI}/data/companies/companies.parquet"
    try:
        companies_df = pd.read_parquet(
            companies_remote_path, filesystem=fs, columns=COMPANY_COLUMNS
        )
        n = upsert_dataframe(engine, companies_table, companies_df, "unique_id")
        print(f"Synced {n} companies")
    except FileNotFoundError:
        print("companies.parquet not found in bucket — skipping companies sync")

    company_ids = target_company_ids(engine)
    print(f"Tracking {len(company_ids)} target-industry companies")

    total_jobs_synced = 0
    try:
        for date in target_dates:
            date_str = date.strftime("%Y-%m-%d")
            remote_path = f"{BUCKET_URI}/data/{variant}/changes/{date_str}.parquet"

            try:
                changes_df = pd.read_parquet(remote_path, filesystem=fs)
            except FileNotFoundError:
                print(f"  {date_str}: no delta file, skipping")
                continue

            changes_df = filter_target_jobs(prepare_jobs_df(changes_df), company_ids)
            changes_df = add_city_from_full(
                fs, f"{BUCKET_URI}/data/full/changes/{date_str}.parquet", changes_df
            )
            n = upsert_dataframe(engine, jobs_table, changes_df, "unique_id")
            total_jobs_synced += n
            print(f"  {date_str}: synced {n} matching job records")
    except Exception as exc:
        log_sync_run(engine, "sync", total_jobs_synced, started,
                     status="error", detail=str(exc)[:500])
        raise

    print(f"\nDone. Total job records upserted: {total_jobs_synced}")
    log_sync_run(engine, "sync", total_jobs_synced, started)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--variant", choices=["minimal", "full"], default="minimal")
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help=f"Sync every delta from {EARLIEST_KNOWN_DELTA} through today",
    )
    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date
    if args.backfill and start_date is None:
        start_date = EARLIEST_KNOWN_DELTA

    sync_database_with_bucket(
        variant=args.variant, start_date=start_date, end_date=end_date
    )


if __name__ == "__main__":
    main()
