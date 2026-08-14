"""Start a throwaway Postgres for local development and publish its conninfo.

The container is recreated on every run so the live schema can never drift from
ops/postgres/init.sql -- the drift that happens when a long-lived database is
initialised once by hand and later schema changes are never applied. Nothing is
bound to a fixed host port either, so this project never competes with (or
accidentally connects to) another local stack's Postgres.

The testcontainers reaper (Ryuk) is switched off: it bind-mounts the Docker
socket, which fails on Docker Desktop's ~/.docker/run/docker.sock, and this
script already removes its own container on exit. The trade-off is that a
SIGKILL leaks the container -- see the README for the cleanup command.
"""

import signal
import sys
import threading
import time
from pathlib import Path

import psycopg
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.config import testcontainers_config

testcontainers_config.ryuk_disabled = True

IMAGE = "postgres:17-alpine"
DB_USER = "celery"
DB_PASSWORD = "celery"
DB_NAME = "celery_viewer"

REPO_ROOT = Path(__file__).parents[2]
SCHEMA_FILE = REPO_ROOT / "ops" / "postgres" / "init.sql"
ENV_FILE = REPO_ROOT / ".dev-db.env"

READY_ATTEMPTS = 30
READY_DELAY_SECONDS = 1


def _wait_until_reachable(conninfo: str) -> None:
    """Poll the published host port instead of trusting the container's own readiness check.

    testcontainers verifies Postgres by running psql *inside* the container, which
    says nothing about whether the mapped host port is accepting connections yet.
    """
    last_error: Exception | None = None
    for _ in range(READY_ATTEMPTS):
        try:
            with psycopg.connect(conninfo, connect_timeout=2):
                return
        except psycopg.OperationalError as exc:
            last_error = exc
            time.sleep(READY_DELAY_SECONDS)
    raise TimeoutError(f"Postgres not reachable on the host after {READY_ATTEMPTS} attempts") from last_error


def _apply_schema(conninfo: str) -> None:
    """Send init.sql as a single statement so psycopg uses the simple-query protocol.

    That protocol lets the server parse the whole multi-statement file, including
    the dollar-quoted set_updated_at() body, without any client-side splitting.
    """
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute(SCHEMA_FILE.read_text())


def _block_until_signalled() -> None:
    """Wait on an Event released by SIGINT/SIGTERM rather than catching KeyboardInterrupt.

    A bare KeyboardInterrupt only covers Ctrl+C in a terminal; when this runs under
    a process supervisor or a wrapper shell the stop arrives as SIGTERM, and
    without a handler the container and .dev-db.env would both be left behind.
    """
    stop = threading.Event()
    for signal_number in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signal_number, lambda *_: stop.set())
    stop.wait()


def main() -> int:
    with PostgresContainer(
        IMAGE,
        username=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        driver=None,
    ) as postgres:
        conninfo = postgres.get_connection_url()
        try:
            _wait_until_reachable(conninfo)
            _apply_schema(conninfo)
            ENV_FILE.write_text(f"PG_CONNINFO={conninfo}\n")

            print(f"container   {postgres.get_wrapped_container().name}")
            print(f"host port   {postgres.get_exposed_port(5432)}")
            print(f"conninfo    {conninfo}")
            print(f"published   {ENV_FILE.relative_to(REPO_ROOT)}")
            print(f"schema      applied {SCHEMA_FILE.relative_to(REPO_ROOT)}")
            print("ready -- Ctrl+C to stop and remove the container")

            _block_until_signalled()
        finally:
            print("\nstopping")
            ENV_FILE.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
