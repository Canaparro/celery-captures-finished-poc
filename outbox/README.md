# Outbox — Celery tasks + workflow tracking

This package is the worker-side of the POC: Celery task definitions, a Redis-set workflow coordinator, and result publishing enriched with the fields the viewer needs.

## What lives here

- **`celery_app.py`** — Celery app, `process_record` task, and `publish_result_to_queue`.
- **`redis_counter.py`** — two Redis sets per workflow (`created` / `completed`) + atomic completion check (Lua). Exposes the shared `redis_client`.
- **`send_tasks.py`** — CLI entry point. Builds a random task tree of a given size and submits the root. Sets `workflow:current` in Redis so the viewer's default view finds it.

## How the workflow tracker works

1. `send_tasks.py` generates a `workflow_id`, clears the sets, SETs `workflow:current`, and publishes the root task.
2. When `process_record` spawns children in `process_children`, each child's deterministic task_id is added to `workflow:{id}:created` before `apply_async`.
3. Each child is spawned with **`kwargs={'parent_task_id': task.request.id}`** so the child knows its parent.
4. On a terminal state (`success` / `permanent_failure` / `transient_failure_exhausted`) the worker publishes a result and `consume_results.py` calls `add_task_completed`, which atomically SADDs to the completed set and checks if `created == completed`. When equal, the workflow is complete.

## Result payload shape

`publish_result_to_queue` sends one JSON message per status change to the `task_results` RabbitMQ queue:

```json
{
  "workflow_id":    "…",
  "task_id":        "…",
  "parent_task_id": "…" or null,
  "record_id":      1,
  "status":         "success | permanent_failure | transient_failure | transient_failure_exhausted",
  "message":        "Processed record 1",
  "attempt":        0,
  "first_seen_at":  "2026-05-12T05:24:41.311892+00:00",
  "timestamp":      "2026-05-12T05:24:44.343040+00:00"
}
```

- `parent_task_id` is `null` only for the root task.
- `first_seen_at` is captured once via `SETNX task:{task_id}:first_seen_at` and reused across retries, so the consumer can compute an accurate `total_duration_ms` (finished − first_seen).
- `attempt` is the 0-indexed retry attempt at the time of publish; for a first-try success it's `0`, for `transient_failure_exhausted` it's `max_retries - 1`.

## Task ID generation

`generate_task_id(workflow_id, task_name, record_id)` hashes those three fields to a Celery-shaped UUID string. Same inputs → same task_id, which makes UPSERTs in Postgres naturally idempotent.

## Running

Prerequisites: RabbitMQ on `localhost:5682`, Redis on `localhost:6389` (see root README).

```bash
# Terminal 1 — worker
poetry run celery -A outbox.celery_app worker --pool=threads --concurrency=4 --loglevel=info

# Terminal 2 — consumer (persists to Postgres)
poetry run python consume_results.py

# Terminal 3 — submit a sample workflow (defaults to 5 randomly-shaped tasks)
poetry run python -m outbox.send_tasks
```

### Tuning the workflow

`send_tasks` generates a random tree. All knobs are optional:

| Flag                          | Default | What it does                                               |
| ----------------------------- | ------- | ---------------------------------------------------------- |
| `--count N`                   | `5`     | Total tasks in the tree                                    |
| `--seed N`                    | random  | RNG seed — same seed + args ⇒ identical tree               |
| `--max-children N`            | `4`     | Max children per node (controls depth vs. breadth)         |
| `--transient-failure-rate F`  | `0.15`  | P(node is marked `transient_failure`) — retries 2× then OK |
| `--permanent-failure-rate F`  | `0.05`  | P(node is marked `permanent_failure`) — fails immediately  |

Examples:

```bash
# wide, shallow, mostly clean
poetry run python -m outbox.send_tasks --count 12 --max-children 6 \
  --transient-failure-rate 0.05 --permanent-failure-rate 0

# deep chain with lots of retries, reproducible
poetry run python -m outbox.send_tasks --count 20 --max-children 2 \
  --transient-failure-rate 0.4 --seed 42
```

## Inspecting live state in Redis

```bash
redis-cli -p 6389 KEYS 'workflow:*'
redis-cli -p 6389 SMEMBERS workflow:<workflow_id>:created
redis-cli -p 6389 SMEMBERS workflow:<workflow_id>:completed
redis-cli -p 6389 SDIFF   workflow:<workflow_id>:created workflow:<workflow_id>:completed
redis-cli -p 6389 GET     workflow:current
```

## Why two tracking systems?

Redis is the **live coordinator** — atomic, fast, ideal for deciding "is this workflow done yet?" at task-completion rate. Postgres (written by `consume_results.py`) is the **durable, queryable store** for the viewer. They do not overlap: Redis knows what's pending right now; Postgres knows everything that has ever finished.
