# Celery Task Queue POC

A proof of concept for Celery with RabbitMQ that demonstrates:
- Using RabbitMQ as the message broker for task distribution
- Publishing task results to a separate RabbitMQ queue
- Consuming results from the queue asynchronously

## Architecture

```
[send_tasks.py] → [RabbitMQ: celery queue] → [Celery Worker]
                                                     ↓
                    [consume_results.py] ← [RabbitMQ: task_results queue]
```

Tasks are sent to Celery workers via RabbitMQ. Instead of storing results in a backend (like Redis), workers publish results directly to a `task_results` queue in RabbitMQ, which is consumed by a separate listener process.

## Setup

### 1. Install Dependencies

```bash
poetry install
```

### 2. Start RabbitMQ

**Using Docker:**
```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

**Using Homebrew (macOS):**
```bash
brew services start rabbitmq
```

The RabbitMQ management UI will be available at http://localhost:15672 (guest/guest)

## Running the Application

You'll need **3 terminals** to run the complete setup:

### Terminal 1: Start the Celery Worker

Start the Celery worker with thread pool (4 threads):

```bash
poetry run celery -A celery_app worker --pool=threads --concurrency=4 --loglevel=info
```

### Terminal 2: Start the Results Consumer

Start the consumer that listens for results:

```bash
poetry run python consume_results.py
```

This will continuously listen to the `task_results` queue and print results as they arrive.

### Terminal 3: Send Tasks

Send tasks to the worker:

```bash
poetry run python send_tasks.py
```

You should see results appearing in Terminal 2 (the consumer).

## Files

- `celery_app.py` - Celery configuration and task definitions
- `send_tasks.py` - Script to send tasks to the worker
- `consume_results.py` - Consumer that listens to the results queue and prints results
- `pyproject.toml` - Project dependencies

## How It Works

1. **Task Submission**: `send_tasks.py` sends tasks to the Celery queue in RabbitMQ
2. **Task Execution**: Celery worker picks up tasks and executes them
3. **Result Publishing**: After execution, the worker publishes results to the `task_results` queue
4. **Result Consumption**: `consume_results.py` consumes messages from the `task_results` queue and displays them

This pattern is useful when you want to:
- Decouple result handling from task execution
- Have multiple consumers processing results
- Stream results as they become available
- Avoid polling for results
