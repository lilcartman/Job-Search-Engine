"""
Optional enrichment step: fetches the `full` variant's description field only
for jobs already in our DB that matched the target-role filters, instead of
syncing the entire full dataset.

This keeps storage/bandwidth proportional to jobs we actually care about.

Usage:
    python -m etl.fetch_full_for_matches
"""

import os

import pandas as pd
from dotenv import load_dotenv
from huggingface_hub import HfFileSystem
from sqlalchemy import create_engine, text

load_dotenv()

BUCKET_URI = "hf://buckets/Invicto69/Jobs-Dataset-bucket"
DATABASE_URL = os.getenv("DATABASE_URL")

GET_MATCHED_UNIQUE_IDS = """
SELECT j.unique_id
FROM jobs j
JOIN target_role_jobs t ON t.id = j.id
WHERE j.description IS NULL;
"""

UPDATE_DESCRIPTION = text(
    "UPDATE jobs SET description = :description WHERE unique_id = :unique_id"
)


def main():
    if not DATABASE_URL:
        raise SystemExit(
            "DATABASE_URL is not set. Copy .env.example to .env and fill it in."
        )

    engine = create_engine(DATABASE_URL)

    with engine.connect() as conn:
        matched_ids = {
            row[0] for row in conn.execute(text(GET_MATCHED_UNIQUE_IDS)).fetchall()
        }

    if not matched_ids:
        print("No matched jobs are missing descriptions. Nothing to do.")
        return

    print(f"Fetching full descriptions for {len(matched_ids)} matched jobs...")

    fs = HfFileSystem()
    full_shards_path = f"{BUCKET_URI}/data/full"
    shard_paths = [
        p for p in fs.ls(full_shards_path, detail=False) if p.endswith(".parquet")
    ]

    updated = 0
    with engine.begin() as conn:
        for shard_path in shard_paths:
            df = pd.read_parquet(
                shard_path,
                filesystem=fs,
                columns=["unique_id", "description"],
            )
            df = df[df["unique_id"].isin(matched_ids)]
            for _, row in df.iterrows():
                conn.execute(
                    UPDATE_DESCRIPTION,
                    {"description": row["description"], "unique_id": row["unique_id"]},
                )
                updated += 1

    print(f"Updated descriptions for {updated} jobs.")


if __name__ == "__main__":
    main()
