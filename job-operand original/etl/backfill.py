"""
One-time streaming backfill of the base dataset into Postgres.

Reads each base parquet shard directly from the public Hugging Face bucket
into memory (nothing is written to local disk), keeps only jobs whose titles
match TARGET_TITLE_PATTERNS, and upserts the matches plus the trimmed
companies lookup table into Neon.

Run once, then use etl/sync.py for daily deltas going forward.

Usage:
    python -m etl.backfill
"""

import pandas as pd
from huggingface_hub import HfFileSystem
from sqlalchemy import create_engine, MetaData, Table

from etl.sync import (
    BUCKET_URI,
    COMPANY_COLUMNS,
    DATABASE_URL,
    add_city_from_full,
    filter_target_jobs,
    prepare_jobs_df,
    upsert_dataframe,
)

CHUNK_SIZE = 5000


def chunked_upsert(engine, table, df, conflict_col):
    for start in range(0, len(df), CHUNK_SIZE):
        upsert_dataframe(engine, table, df.iloc[start:start + CHUNK_SIZE], conflict_col)
    return len(df)


def main():
    if not DATABASE_URL:
        raise SystemExit(
            "DATABASE_URL is not set. Copy .env.example to .env and fill it in."
        )

    fs = HfFileSystem()  # public bucket, no token needed
    engine = create_engine(DATABASE_URL)
    metadata = MetaData()
    companies_table = Table("companies", metadata, autoload_with=engine)
    jobs_table = Table("jobs", metadata, autoload_with=engine)

    # Companies first, so jobs.company_id always has a row to point at.
    companies_df = pd.read_parquet(
        f"{BUCKET_URI}/data/companies/companies.parquet",
        filesystem=fs,
        columns=COMPANY_COLUMNS,
    )
    n = chunked_upsert(engine, companies_table, companies_df, "unique_id")
    print(f"Synced {n} companies")

    shard_paths = sorted(
        p for p in fs.ls(f"{BUCKET_URI}/data/minimal", detail=False)
        if p.endswith(".parquet")
    )
    print(f"Found {len(shard_paths)} base shards")

    total_matched = 0
    for remote_path in shard_paths:
        df = pd.read_parquet(f"hf://{remote_path}", filesystem=fs)
        matches = filter_target_jobs(prepare_jobs_df(df))
        # Each minimal base shard has a same-named full-variant twin holding
        # job_model_json; pull city from it for just the matched ids.
        full_path = f"hf://{remote_path}".replace("/data/minimal/", "/data/full/")
        matches = add_city_from_full(fs, full_path, matches)
        n = chunked_upsert(engine, jobs_table, matches, "unique_id")
        total_matched += n
        print(f"  {remote_path}: {n} matches out of {len(df)} rows")

    print(f"\nBackfill complete. Total matching jobs stored: {total_matched}")


if __name__ == "__main__":
    main()
