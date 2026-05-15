# Viewer — FastAPI backend

Serves two things: the JSON API the React frontend queries, and the built frontend itself as static files.

## Files

- **`api.py`** — FastAPI app, endpoints, static mount.
- **`db.py`** — shared `psycopg_pool.ConnectionPool` used by the endpoints.

## Endpoints

| Method | Path                                                               | Purpose |
| ------ | ------------------------------------------------------------------ | ------- |
| GET    | `/api/workflows/current`                                           | Read `workflow:current` from Redis. 404 if none. |
| GET    | `/api/workflows/{workflow_id}/tasks`                               | All persisted tasks (id, parent, record_id, final_status, metrics). Ordered by `created_at`. |
| GET    | `/api/workflows/{workflow_id}/tasks/{task_id}`                     | Single task including `last_message`, full metrics, timestamps. |
| GET    | `/api/workflows/{workflow_id}/highlights?by=slowest\|most_retries\|failed&limit=N` | Server-side sorted/filtered list of task_ids for the "find interesting nodes" UI. |
| GET    | `/` and everything else                                            | Static files from `web/dist/` (built by `cd web && npm run build`). |

The static mount is declared **after** the API routes so `/api/*` keeps precedence.

## Connection details

Hard-coded in `viewer/db.py` and `viewer/api.py` to match the POC's convention:

- Postgres: `postgresql://celery:celery@localhost:5442/celery_viewer`
- Redis: `localhost:6389`

If you move services, change those constants — or factor them to environment variables.

## Running

```bash
poetry run uvicorn viewer.api:app --host 127.0.0.1 --port 8000
```

On startup the connection pool opens and waits for Postgres to be reachable. On shutdown it's closed cleanly.

## Manual smoke test

```bash
# current workflow id
curl -s http://127.0.0.1:8000/api/workflows/current

# all tasks for a workflow
curl -s http://127.0.0.1:8000/api/workflows/<id>/tasks | jq '.[].record_id'

# one task detail
curl -s http://127.0.0.1:8000/api/workflows/<id>/tasks/<task_id> | jq .

# highlight queries
curl -s 'http://127.0.0.1:8000/api/workflows/<id>/highlights?by=slowest&limit=3'
curl -s 'http://127.0.0.1:8000/api/workflows/<id>/highlights?by=most_retries'
curl -s 'http://127.0.0.1:8000/api/workflows/<id>/highlights?by=failed'
```

## Data model contract with the frontend

Tasks returned by `/tasks` and `/tasks/{id}` use the bundled-metrics shape — business fields (`task_id`, `parent_task_id`, `record_id`, `final_status`, `last_message`) are top-level, while execution metrics (`retry_count`, `total_duration_ms`, `first_seen_at`, `finished_at`) live under `metrics`. The separation exists so the result isn't confused with the metrics about how the result was produced.

## Workflow is the root

The frontend treats `workflow_id` itself as a node. Every task (including the first one) has `parent_task_id` set — for the first task, its parent **is** the workflow id. This guarantees the graph always has one root (the workflow), even if the app later spawns multiple top-level tasks per workflow.
