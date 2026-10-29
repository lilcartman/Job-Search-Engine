"""
Utah + Remote engine: the final filter on top of target_role_jobs.

Reads the view straight from Neon, keeps rows that are Utah-related or
remote-friendly, tags each with WHY it matched, then writes the shortlist
two ways: a CSV next to this project, and a utah_remote_jobs table back in
Neon (which the dashboard picks up automatically).

Matching logic:
  Utah  — city in a gazetteer of Utah cities (unambiguous ones count alone;
          ambiguous names like Sandy/Logan also need a Utah context signal
          on the same row), 'utah'/'silicon slopes' anywhere in city,
          country, or title, or a word-boundary ', UT' abbreviation.
  Remote — remote column is true, OR workplace_type says remote, OR the
          word 'remote' appears in the title or (people do this) the city.

The output table is derived data — safe to regenerate any time; each run
fully replaces it.

Usage:
    python -m scripts.utah_remote_scan
"""

import os
import re

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
CSV_PATH = "utah_remote_jobs.csv"
TABLE_NAME = "utah_remote_jobs"

# Utah cities distinctive enough to count on their own.
UTAH_CITIES = [
    "salt lake city", "slc", "west valley city", "provo", "orem", "lehi",
    "ogden", "st. george", "st george", "saint george", "park city",
    "american fork", "spanish fork", "pleasant grove", "springville",
    "cottonwood heights", "cedar city", "taylorsville", "millcreek",
    "herriman", "eagle mountain", "tooele", "kaysville", "midvale",
    "south jordan", "west jordan", "bountiful", "brigham city",
    "bluffdale", "lindon", "holladay", "heber city", "moab",
]

# City names Utah shares with other states — these only count when the same
# row also shows a Utah context signal (', UT', 'utah', etc.).
UTAH_CITIES_AMBIGUOUS = [
    "sandy", "draper", "murray", "logan", "riverton", "layton",
    "clearfield", "farmington", "saratoga springs", "payson", "hurricane",
    "washington city",
]

UTAH_WORDS = ["utah", "silicon slopes"]

# ', UT' / ' UT' as its own word — NOT the letters inside 'executive'.
UT_ABBREV = re.compile(r"(?i)(?:^|[,\s(])ut\.?(?:$|[),\s])")


def _word_re(names: list[str]) -> re.Pattern:
    return re.compile(r"(?i)\b(?:" + "|".join(re.escape(n) for n in names) + r")\b")


def scan(df: pd.DataFrame) -> pd.DataFrame:
    """Returns matching rows with a match_reason column explaining each."""
    city = df["city"].fillna("")
    country = df["country"].fillna("")
    title = df["title"].fillna("")
    place = city + " | " + country          # the two location fields together
    everything = place + " | " + title      # ...plus the title

    utah_words = everything.str.contains(_word_re(UTAH_WORDS), na=False)
    ut_abbrev = place.apply(lambda s: bool(UT_ABBREV.search(s)))
    utah_context = utah_words | ut_abbrev

    utah_city = city.str.contains(_word_re(UTAH_CITIES), na=False)
    utah_city_amb = city.str.contains(_word_re(UTAH_CITIES_AMBIGUOUS), na=False) & utah_context

    remote_flag = df["remote"].fillna(False).astype(bool)
    remote_type = df["workplace_type"].fillna("").str.lower().eq("remote")
    remote_title = title.str.contains(r"(?i)\bremote\b", na=False)
    remote_city = city.str.contains(r"(?i)\bremote\b", na=False)

    signals = {
        "utah_city": utah_city,
        "utah_city_ctx": utah_city_amb,
        "utah_text": utah_words,
        "ut_abbrev": ut_abbrev,
        "remote_flag": remote_flag,
        "remote_type": remote_type,
        "remote_title": remote_title,
        "remote_city": remote_city,
    }

    any_utah = utah_city | utah_city_amb | utah_words | ut_abbrev
    any_remote = remote_flag | remote_type | remote_title | remote_city
    keep = any_utah | any_remote

    out = df[keep].copy()
    reasons = pd.DataFrame({k: v[keep] for k, v in signals.items()})
    out["match_reason"] = reasons.apply(
        lambda row: "+".join(name for name, hit in row.items() if hit), axis=1
    )
    out["is_utah"] = any_utah[keep]

    # Utah hits first (rarer, higher intent), newest postings first within each.
    out = out.sort_values(
        ["is_utah", "posted_at"], ascending=[False, False], na_position="last"
    ).drop(columns=["is_utah"])
    return out


def main():
    if not DATABASE_URL:
        raise SystemExit(
            "DATABASE_URL is not set. Copy .env.example to .env and fill it in."
        )

    engine = create_engine(DATABASE_URL)
    df = pd.read_sql("SELECT * FROM target_role_jobs", engine)
    matches = scan(df)

    n_utah = matches["match_reason"].str.contains("utah|ut_abbrev").sum()
    print(f"View rows scanned: {len(df)}")
    print(f"Kept: {len(matches)}  (Utah-related: {n_utah}, "
          f"remote-only: {len(matches) - n_utah})")

    cols = ["title", "company_name", "city", "country", "remote",
            "workplace_type", "match_reason", "apply_url", "posted_at"]
    matches[cols].to_csv(CSV_PATH, index=False)
    print(f"CSV written: {CSV_PATH}")

    matches[cols].to_sql(TABLE_NAME, engine, if_exists="replace", index=False)
    print(f"Neon table refreshed: {TABLE_NAME} (visible in the dashboard)")

    print("\nTop 10:")
    for r in matches.head(10).itertuples():
        print(f"- {r.title} @ {r.company_name} | {r.city or '?'}, "
              f"{r.country or '?'} [{r.match_reason}]")


if __name__ == "__main__":
    main()
