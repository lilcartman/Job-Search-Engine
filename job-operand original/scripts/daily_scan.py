"""
Runs the target-role SQL view and reports jobs. Meant to be run on a
schedule — a cron job, or handed to an agent (e.g. Claude Cowork) as a daily
task alongside `python -m etl.sync`.

It tracks which job IDs it has already reported (in a local `.last_seen`
file) so repeated runs only surface genuinely new matches.

Usage:
    python -m scripts.daily_scan            # only show new matches since last run
    python -m scripts.daily_scan --all       # show everything currently open
"""

import argparse
import json
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
SEEN_FILE = ".last_seen_job_ids.json"

VIEW_QUERY = """
SELECT id, title, company_name, department, country, city, remote,
       apply_url, posted_at
FROM target_role_jobs
ORDER BY posted_at DESC NULLS LAST;
"""


def load_seen_ids():
    if not os.path.exists(SEEN_FILE):
        return set()
    with open(SEEN_FILE) as f:
        return set(json.load(f))


def save_seen_ids(ids):
    with open(SEEN_FILE, "w") as f:
        json.dump(sorted(ids), f)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--all", action="store_true", help="Show all current matches, not just new ones"
    )
    args = parser.parse_args()

    if not DATABASE_URL:
        raise SystemExit(
            "DATABASE_URL is not set. Copy .env.example to .env and fill it in."
        )

    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        rows = conn.execute(text(VIEW_QUERY)).mappings().all()

    seen_ids = load_seen_ids()
    current_ids = {row["id"] for row in rows}

    to_show = rows if args.all else [r for r in rows if r["id"] not in seen_ids]

    if not to_show:
        print("No new target-role jobs since last run.")
    else:
        label = "matches" if args.all else "new matches"
        print(f"{len(to_show)} {label}:\n")
        for row in to_show:
            remote_tag = " (remote)" if row["remote"] else ""
            print(f"- {row['title']} @ {row['company_name']}{remote_tag}")
            location = ", ".join(p for p in (row["city"], row["country"]) if p)
            if location:
                print(f"    {location}")
            if row["apply_url"]:
                print(f"    {row['apply_url']}")
            print()

    save_seen_ids(current_ids | seen_ids)


if __name__ == "__main__":
    main()
