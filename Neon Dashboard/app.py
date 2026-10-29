"""
Cowork pipeline dashboard — a small read-only web console for a Neon Postgres database.

Run:  uvicorn app:app --reload
Needs: DATABASE_URL in the environment or in a .env file next to this script.
"""

import os
from datetime import date, datetime
from decimal import Decimal

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL is not set. Copy .env.example to .env and paste your "
        "Neon connection string (Neon console → Connect → connection string)."
    )

# Small pool: Neon connections are slow to open, so keep a couple warm.
pool = ConnectionPool(DATABASE_URL, min_size=1, max_size=4, kwargs={"row_factory": dict_row})

app = FastAPI(title="Cowork dashboard")

PER_PAGE = 50


def jsonable(value):
    """Make Postgres values JSON-friendly."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def get_schema() -> dict[str, list[dict]]:
    """Map of table name -> list of {name, type} for all public tables."""
    sql = """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position
    """
    schema: dict[str, list[dict]] = {}
    with pool.connection() as conn:
        for row in conn.execute(sql):
            schema.setdefault(row["table_name"], []).append(
                {"name": row["column_name"], "type": row["data_type"]}
            )
    return schema


@app.get("/")
def index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))


@app.get("/api/overview")
def overview():
    """Tables with row counts, plus the latest sync_log entry if that table exists."""
    schema = get_schema()
    # Display filter: the console only surfaces the Utah/Remote shortlist.
    schema = {name: cols for name, cols in schema.items() if name == "utah_remote_jobs"}
    tables = []
    with pool.connection() as conn:
        for name in schema:
            # Table names come from information_schema, not user input, so
            # quoting them directly here is safe.
            count = conn.execute(f'SELECT count(*) AS n FROM "{name}"').fetchone()["n"]
            tables.append({"name": name, "rows": count, "columns": schema[name]})

        last_sync = None
        if "sync_log" in schema:
            row = conn.execute(
                'SELECT * FROM "sync_log" ORDER BY 1 DESC LIMIT 1'
            ).fetchone()
            if row:
                last_sync = {k: jsonable(v) for k, v in row.items()}

    tables.sort(key=lambda t: t["name"])
    return {"tables": tables, "last_sync": last_sync}


@app.get("/api/table/{name}")
def table_data(
    name: str,
    page: int = Query(1, ge=1),
    search: str = "",
    sort: str = "",
    dir: str = Query("asc", pattern="^(asc|desc)$"),
):
    schema = get_schema()
    if name not in schema:
        raise HTTPException(404, f"No table named {name!r}")

    columns = schema[name]
    col_names = [c["name"] for c in columns]

    # Sort column must be a real column of this table.
    if sort and sort not in col_names:
        raise HTTPException(400, f"Cannot sort by {sort!r}")

    where = ""
    params: list = []
    if search:
        text_cols = [
            c["name"] for c in columns
            if c["type"] in ("text", "character varying", "character")
        ]
        if text_cols:
            where = "WHERE " + " OR ".join(f'"{c}" ILIKE %s' for c in text_cols)
            params = [f"%{search}%"] * len(text_cols)

    order = f'ORDER BY "{sort}" {dir.upper()}' if sort else ""
    offset = (page - 1) * PER_PAGE

    with pool.connection() as conn:
        total = conn.execute(
            f'SELECT count(*) AS n FROM "{name}" {where}', params
        ).fetchone()["n"]
        rows = conn.execute(
            f'SELECT * FROM "{name}" {where} {order} LIMIT {PER_PAGE} OFFSET {offset}',
            params,
        ).fetchall()

    return {
        "columns": col_names,
        "rows": [{k: jsonable(v) for k, v in r.items()} for r in rows],
        "total": total,
        "page": page,
        "per_page": PER_PAGE,
    }
