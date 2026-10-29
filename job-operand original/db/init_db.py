"""
Creates all tables defined in db/models.py against DATABASE_URL.

Usage:
    python -m db.init_db
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

from db.models import Base

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")


def main():
    if not DATABASE_URL:
        raise SystemExit(
            "DATABASE_URL is not set. Copy .env.example to .env and fill it in."
        )

    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    print(f"Tables created (or already existed) at: {DATABASE_URL.split('@')[-1]}")


if __name__ == "__main__":
    main()
