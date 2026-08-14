"""Shared Postgres pool for the result consumer and the viewer API.

The dev database runs on a random host port (see ops/postgres/dev_db.py), so the
conninfo cannot be a constant and there is deliberately no fallback default:
silently connecting to whatever sits on a well-known port is what let this
project talk to an unrelated stack's Postgres and miss its own schema.
"""

import os
from pathlib import Path

from psycopg_pool import ConnectionPool

ENV_FILE = Path(__file__).parent / ".dev-db.env"
CONNINFO_KEY = "PG_CONNINFO"

MISSING_CONNINFO_MESSAGE = (
    f"No {CONNINFO_KEY} found. Start the dev database with "
    "`poetry run python ops/postgres/dev_db.py`, or set "
    f"{CONNINFO_KEY} to point at your own Postgres."
)


def _read_env_file(key: str) -> str | None:
    if not ENV_FILE.exists():
        return None
    for line in ENV_FILE.read_text().splitlines():
        name, _, value = line.partition("=")
        if name.strip() == key:
            return value.strip()
    return None


def resolve_conninfo() -> str:
    conninfo = os.environ.get(CONNINFO_KEY) or _read_env_file(CONNINFO_KEY)
    if not conninfo:
        raise RuntimeError(MISSING_CONNINFO_MESSAGE)
    return conninfo


PG_CONNINFO = resolve_conninfo()

PG_POOL = ConnectionPool(
    conninfo=PG_CONNINFO,
    min_size=1,
    max_size=4,
    kwargs={"autocommit": True},
    open=False,
)
