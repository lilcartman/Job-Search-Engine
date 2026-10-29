"""
One-time enrichment: populate jobs.city for rows already in Neon.

Scans the FULL variant (base shards, then deltas newest-first) for the
unique_ids that still have city IS NULL. Per file it first reads only the
cheap 'id' column to see which needed ids are present, and only then does a
filtered read of job_model_json for those ids — the full files carry every
job's description inside the JSON, so unfiltered reads would be gigabytes.

An id is considered resolved once found in any file (location is static),
even if its JSON has no city — avoids re-fetching hopeless rows.

Usage:
    python -m etl.enrich_city
"""

import pandas as pd
from huggingface_hub import HfFileSystem
from sqlalchemy import create_engine, text

from etl.sync import BUCKET_URI, DATABASE_URL, _extract_city


def full_file_paths(fs):
    """Base shards first (bulk of history), then deltas newest-first."""
    shards = sorted(
        p for p in fs.ls(f"{BUCKET_URI}/data/full", detail=False)
        if p.endswith(".parquet")
    )
    deltas = sorted(
        fs.ls(f"{BUCKET_URI}/data/full/changes", detail=False), reverse=True
    )
    return [f"hf://{p}" for p in shards + deltas]


def main():
    if not DATABASE_URL:
        raise SystemExit(
            "DATABASE_URL is not set. Copy .env.example to .env and fill it in."
        )

    fs = HfFileSystem()
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT unique_id FROM jobs WHERE city IS NULL")
        ).scalars().all()
    remaining = set(rows)
    print(f"{len(remaining)} rows need city")

    for path in full_file_paths(fs):
        if not remaining:
            break

        ids_here = pd.read_parquet(path, filesystem=fs, columns=["id"])
        wanted = remaining & set(ids_here["id"])
        short = path.rsplit("/", 1)[-1]
        if not wanted:
            print(f"  {short}: 0 needed ids")
            continue

        full_df = pd.read_parquet(
            path,
            filesystem=fs,
            columns=["id", "job_model_json"],
            filters=[("id", "in", list(wanted))],
        )
        updates = [
            {"uid": row.id, "city": _extract_city(row.job_model_json)}
            for row in full_df.itertuples()
        ]
        with_city = [u for u in updates if u["city"] is not None]
        if with_city:
            with engine.begin() as conn:
                conn.execute(
                    text("UPDATE jobs SET city = :city WHERE unique_id = :uid"),
                    with_city,
                )
        remaining -= {u["uid"] for u in updates}
        print(f"  {short}: updated {len(with_city)}, "
              f"{len(updates) - len(with_city)} had no city; "
              f"{len(remaining)} still unresolved")

    print(f"\nDone. {len(remaining)} ids never found in any full file.")


if __name__ == "__main__":
    main()
