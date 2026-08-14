import os
from contextlib import asynccontextmanager

import redis
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles

from db import PG_POOL

REDIS_HOST = "localhost"
REDIS_PORT = 6389
WEB_DIST = os.path.join(os.path.dirname(os.path.dirname(__file__)), "web", "dist")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    PG_POOL.open()
    PG_POOL.wait()
    yield
    PG_POOL.close()


app = FastAPI(lifespan=lifespan)


def _rows(cur) -> list[dict]:
    cols = [c.name for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


@app.get("/api/workflows/current")
def current_workflow():
    wf = r.get("workflow:current")
    if not wf:
        raise HTTPException(404, "no current workflow")
    return {"workflow_id": wf}


@app.get("/api/workflows/{workflow_id}/status")
def workflow_status(workflow_id: str):
    """Pulls status from the workflows table, populated by consume_results.py.
    Returns 'unknown' if no row exists yet (e.g. consumer hasn't seen the first
    terminal event)."""
    sql = """
        SELECT w.status, w.started_at, w.finished_at,
               (SELECT count(*) FROM tasks WHERE workflow_id = %s) AS persisted_tasks
        FROM workflows w
        WHERE w.workflow_id = %s
    """
    with PG_POOL.connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (workflow_id, workflow_id))
        row = cur.fetchone()
        if not row:
            return {
                "workflow_id": workflow_id,
                "status": "unknown",
                "started_at": None,
                "finished_at": None,
                "persisted_tasks": 0,
            }
        cols = [c.name for c in cur.description]
        data = dict(zip(cols, row))
        data["workflow_id"] = workflow_id
        return data


@app.get("/api/workflows/{workflow_id}/tasks")
def list_tasks(workflow_id: str):
    sql = """
        SELECT task_id, parent_task_id, record_id, label, final_status, metrics
        FROM tasks
        WHERE workflow_id = %s
        ORDER BY created_at
    """
    with PG_POOL.connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (workflow_id,))
        return _rows(cur)


@app.get("/api/workflows/{workflow_id}/tasks/{task_id}")
def get_task(workflow_id: str, task_id: str):
    sql = """
        SELECT task_id, parent_task_id, record_id, label, final_status,
               last_message, metrics, created_at, updated_at
        FROM tasks
        WHERE workflow_id = %s AND task_id = %s
    """
    with PG_POOL.connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (workflow_id, task_id))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404)
        cols = [c.name for c in cur.description]
        return dict(zip(cols, row))


@app.get("/api/workflows/{workflow_id}/highlights")
def highlights(workflow_id: str, by: str = "slowest", limit: int = 20):
    base = "SELECT task_id FROM tasks WHERE workflow_id = %s"
    if by == "slowest":
        sql = (
            base
            + " ORDER BY (metrics->>'total_duration_ms')::int DESC NULLS LAST LIMIT %s"
        )
        args = (workflow_id, limit)
    elif by == "most_retries":
        sql = (
            base
            + " AND (metrics->>'retry_count')::int > 0"
            + " ORDER BY (metrics->>'retry_count')::int DESC LIMIT %s"
        )
        args = (workflow_id, limit)
    elif by == "failed":
        sql = (
            base
            + " AND final_status IN ('permanent_failure','transient_failure_exhausted')"
            + " LIMIT %s"
        )
        args = (workflow_id, limit)
    else:
        raise HTTPException(400, "by must be slowest|most_retries|failed")

    with PG_POOL.connection() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        return {"by": by, "task_ids": [row[0] for row in cur.fetchall()]}


if os.path.isdir(WEB_DIST):
    app.mount("/", StaticFiles(directory=WEB_DIST, html=True), name="web")
