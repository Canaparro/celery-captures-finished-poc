# Set-Based Workflow Tracking

This folder contains a workflow tracking approach using Redis sets to track task IDs.

## Architecture

The system uses **two Redis sets** to track workflow tasks:

1. **Created Set** (`workflow:{id}:created`): Task IDs that have been spawned
2. **Completed Set** (`workflow:{id}:completed`): Task IDs that have finished

When a task spawns children, it adds their task IDs to the created set. When a task completes, it adds its ID to the completed set and checks if created == completed (workflow complete).

### Key Operations:

1. **Task Spawning**: Use `apply_async()` to get task ID, add to created set, publish to Celery
2. **Task Completion**: Add task ID to completed set via `on_success`
3. **Workflow Completion**: When all created tasks are in completed set, trigger `process_finished`
4. **Failure Handling**: Permanent failures also add to completed set, allowing workflow to complete

## Components

### `redis_counter.py`
- Manages the Redis sets for workflow tracking
- Functions:
  - `initialize_workflow(workflow_id)`: Clear/initialize the created and completed sets
  - `add_task_created(workflow_id, task_id)`: Add task ID to created set
  - `add_task_completed(workflow_id, task_id)`: Add task ID to completed set, return True if workflow complete
  - `get_pending_tasks(workflow_id)`: Get set of pending task IDs (created - completed)
  - `get_workflow_stats(workflow_id)`: Get counts of created/completed/pending tasks

### `celery_app.py`
- Celery application with task definitions
- `process_record`: Main task that processes records
  - Spawns child tasks via `process_children()`
  - Gets task IDs from `apply_async()` and adds to created set
- `process_finished`: Workflow completion callback
- `CustomTask`: Base class with callbacks
  - `on_success`: Adds task ID to completed set, triggers completion if all done
  - `on_failure`: For permanent failures, adds to completed set

### `send_tasks.py`
- Entry point to start a workflow
- Initializes workflow sets
- Publishes first task and adds its ID to created set

## How to Run

### 1. Start RabbitMQ and Redis
```bash
# Make sure RabbitMQ is running on localhost:5682
# Make sure Redis is running on localhost:6389
```

### 2. Start the Celery Worker
```bash
cd /Users/marcelocanaparro/Documents/source/celery-captures-finished-poc
poetry run celery -A outbox.celery_app worker --pool=threads --concurrency=4 --loglevel=info
```

### 3. Start the Result Consumer (Optional)
In another terminal to see the results:
```bash
cd /Users/marcelocanaparro/Documents/source/celery-captures-finished-poc
poetry run python consume_results.py
```

### 4. Submit a Workflow
In another terminal:
```bash
cd /Users/marcelocanaparro/Documents/source/celery-captures-finished-poc
poetry run python -m outbox.send_tasks
```

## Running from Terminal (Step-by-Step)

Here's a complete walkthrough with all terminal commands:

### Prerequisites

Verify your services are running:
```bash
# Check RabbitMQ (should see management interface or connection)
curl http://localhost:15682/api/overview

# Check Redis
redis-cli -p 6389 ping
# Should return: PONG
```

### Terminal 1: Start Celery Worker

```bash
cd /Users/marcelocanaparro/Documents/source/celery-captures-finished-poc
poetry run celery -A outbox.celery_app worker --pool=threads --concurrency=4 --loglevel=info
```

**What this does:**
- Starts a Celery worker that processes tasks from the queue
- Uses thread pool (better for debugging with breakpoints)
- Concurrency of 4 means it can process 4 tasks in parallel
- You'll see log output as tasks are executed

**Expected output:**
```
[2024-01-30 10:00:00,000: INFO/MainProcess] Connected to amqp://guest:**@localhost:5682//
[2024-01-30 10:00:00,100: INFO/MainProcess] celery@hostname ready.
```

### Terminal 2: Start Result Consumer (Optional but Recommended)

```bash
cd /Users/marcelocanaparro/Documents/source/celery-captures-finished-poc
poetry run python consume_results.py
```

**What this does:**
- Listens to the RabbitMQ `task_results` queue
- Displays task completion messages and workflow status
- Helps you see the workflow progress in real-time

**Expected output:**
```
Starting result consumer...
Connecting to RabbitMQ at localhost:5682
Waiting for results from queue 'task_results'...
Press CTRL+C to exit
```

### Terminal 3: Submit a Workflow

```bash
cd /Users/marcelocanaparro/Documents/source/celery-captures-finished-poc
poetry run python -m outbox.send_tasks
```

**What this does:**
- Creates a workflow with a tree of records
- Generates deterministic task IDs
- Adds first task to Redis created set
- Publishes first task to Celery

**Expected output:**
```
============================================================
Sending tasks to Celery workers...
============================================================
Starting workflow with ID: a1b2c3d4-e5f6-7890-abcd-ef1234567890
Added task f8e7d6c5-b4a3-9281-7065-544332211000 to created set
Published initial task with ID: f8e7d6c5-b4a3-9281-7065-544332211000
============================================================
```

### What Happens Next

1. **In Terminal 1 (Worker):** You'll see tasks being picked up and executed:
   ```
   [2024-01-30 10:00:01,000: INFO] Task process_record[task-id] received
   [2024-01-30 10:00:01,100: INFO] Processing record {...}
   [2024-01-30 10:00:04,200: INFO] Added task {child-id} to created set
   [2024-01-30 10:00:04,300: INFO] Task completed for workflow
   ```

2. **In Terminal 2 (Consumer):** You'll see task results:
   ```
   ============================================================
   Received result:
   "Processed record 1 in workflow abc-123 at 2024-01-30..."
   ============================================================
   ```

3. **When workflow completes:** Final message appears:
   ```
   ============================================================
   Received result:
   "Workflow abc-123 finished at 2024-01-30..."
   ============================================================
   ```

### Verify Workflow Status in Redis

While the workflow is running, you can check Redis:

```bash
redis-cli -p 6389

# Find your workflow
127.0.0.1:6389> KEYS workflow:*

# Check created tasks
127.0.0.1:6389> SMEMBERS workflow:<workflow-id>:created

# Check completed tasks
127.0.0.1:6389> SMEMBERS workflow:<workflow-id>:completed

# See pending tasks (created - completed)
127.0.0.1:6389> SDIFF workflow:<workflow-id>:created workflow:<workflow-id>:completed

# Get counts
127.0.0.1:6389> SCARD workflow:<workflow-id>:created
127.0.0.1:6389> SCARD workflow:<workflow-id>:completed
```

### Troubleshooting

**No tasks processing?**
- Check worker terminal for errors
- Verify RabbitMQ is running: `rabbitmqctl status`
- Check worker is connected to correct broker

**No results appearing?**
- Verify consumer is connected to RabbitMQ
- Check RabbitMQ queue exists: `rabbitmqctl list_queues`

**Workflow not completing?**
- Check Redis sets with commands above
- Look for errors in worker logs
- Verify all tasks moved from created to completed set

## Advantages Over Chords

1. **Failure Resilience**: Unlike chords, if a task fails permanently, the workflow can still complete because we add it to the completed set.

2. **Simplicity**: No need for Celery Beat, dispatcher tasks, or Redis Streams. Just two Redis sets.

3. **Direct Publishing**: Tasks publish directly to Celery - no intermediate queues.

4. **Observability**: You can see exactly which tasks are pending with `get_pending_tasks()` - returns actual task IDs, not just a count.

5. **Debugging**: With task IDs, you can look up specific tasks in Celery results backend or logs.

## Workflow Lifecycle

Example with `Record(id=1, children=[Record(id=3), Record(id=2)])`:

1. **User calls `send_tasks.py`**
   - Initializes sets (empty): `created = {}`, `completed = {}`
   - Publishes `process_record(Record 1)` with task ID `task-1`
   - Adds to created: `created = {task-1}`

2. **Worker executes Record 1 (task-1)**
   - Has 2 children (Record 3, Record 2)
   - Publishes both children and gets their task IDs: `task-3`, `task-2`
   - Adds to created: `created = {task-1, task-3, task-2}`
   - Task completes → `on_success` adds to completed: `completed = {task-1}`
   - Not all tasks done yet (created ≠ completed)

3. **Workers execute Record 3 and Record 2**
   - Record 3 (task-3) completes → adds to completed: `completed = {task-1, task-3}`
   - Still not done (created ≠ completed)
   - Record 2 (task-2) completes → adds to completed: `completed = {task-1, task-3, task-2}`

4. **Sets are equal (workflow complete!)**
   - The Lua script detects `created == completed`
   - Returns `True` to the task that just completed
   - That task calls `process_finished.delay()`
   - Workflow complete!

## Redis State at Each Step

```
Initial:
  created:   {}
  completed: {}

After send_tasks:
  created:   {task-1}
  completed: {}

After task-1 spawns children:
  created:   {task-1, task-3, task-2}
  completed: {}

After task-1 completes:
  created:   {task-1, task-3, task-2}
  completed: {task-1}

After task-3 completes:
  created:   {task-1, task-3, task-2}
  completed: {task-1, task-3}

After task-2 completes:
  created:   {task-1, task-3, task-2}
  completed: {task-1, task-3, task-2}  ← EQUAL! Workflow done!
```

## Error Handling

- **Transient Failures**: Task retries (up to 3 times), not added to completed set until success or permanent failure
- **Permanent Failures**: Task ID added to completed set immediately, workflow can complete
- **Race Conditions**: Lua script atomically adds to completed set AND checks if workflow is complete
