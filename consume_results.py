import json
from datetime import datetime

import pika

from db import PG_CONNINFO, PG_POOL
from outbox.redis_counter import add_task_completed, redis_client

# RabbitMQ configuration
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5682
RABBITMQ_USER = 'guest'
RABBITMQ_PASSWORD = 'guest'
RESULTS_QUEUE = 'task_results'

TERMINAL = {'success', 'permanent_failure', 'transient_failure_exhausted'}

UPSERT_SQL = """
INSERT INTO tasks (task_id, workflow_id, parent_task_id, record_id, label,
                   final_status, last_message, metrics)
VALUES (%(task_id)s, %(workflow_id)s, %(parent_task_id)s, %(record_id)s, %(label)s,
        %(final_status)s, %(last_message)s, %(metrics)s)
ON CONFLICT (task_id) DO UPDATE SET
    final_status   = EXCLUDED.final_status,
    last_message   = EXCLUDED.last_message,
    metrics        = EXCLUDED.metrics,
    parent_task_id = EXCLUDED.parent_task_id,
    record_id      = EXCLUDED.record_id,
    label          = EXCLUDED.label;
"""

# Insert a pending row on first sight of a workflow; do nothing if it already
# exists (so we don't overwrite a 'complete' row with 'pending').
ENSURE_WORKFLOW_SQL = """
INSERT INTO workflows (workflow_id, status)
VALUES (%(workflow_id)s, 'pending')
ON CONFLICT (workflow_id) DO NOTHING;
"""

MARK_WORKFLOW_COMPLETE_SQL = """
UPDATE workflows
   SET status = 'complete', finished_at = now()
 WHERE workflow_id = %(workflow_id)s
   AND status <> 'complete';
"""


def _build_metrics(result: dict) -> dict:
    first_seen_at = result.get('first_seen_at')
    finished_at = result.get('timestamp')
    total_ms = None
    try:
        if first_seen_at and finished_at:
            fs = datetime.fromisoformat(first_seen_at)
            fn = datetime.fromisoformat(finished_at)
            total_ms = int((fn - fs).total_seconds() * 1000)
    except ValueError:
        total_ms = None
    return {
        'retry_count': result.get('attempt', 0),
        'total_duration_ms': total_ms,
        'first_seen_at': first_seen_at,
        'finished_at': finished_at,
    }


def _persist_terminal(result: dict) -> bool:
    """UPSERT one terminal event. Returns False if the payload is malformed
    (missing required fields) — caller should ack-and-skip rather than requeue,
    since the payload will never succeed on retry."""
    if 'record_id' not in result or 'task_id' not in result or 'workflow_id' not in result:
        print(f"Skipping malformed payload (missing required fields): {result}")
        return False
    metrics = _build_metrics(result)
    params = {
        'task_id': result['task_id'],
        'workflow_id': result['workflow_id'],
        'parent_task_id': result.get('parent_task_id'),
        'record_id': result['record_id'],
        'label': result.get('label'),
        'final_status': result['status'],
        'last_message': result.get('message'),
        'metrics': json.dumps(metrics),
    }
    with PG_POOL.connection() as conn, conn.cursor() as cur:
        cur.execute(ENSURE_WORKFLOW_SQL, {'workflow_id': result['workflow_id']})
        cur.execute(UPSERT_SQL, params)
    redis_client.delete(f"task:{result['task_id']}:first_seen_at")
    return True


def _mark_workflow_complete(workflow_id: str) -> None:
    with PG_POOL.connection() as conn, conn.cursor() as cur:
        cur.execute(MARK_WORKFLOW_COMPLETE_SQL, {'workflow_id': workflow_id})


def callback(ch, method, properties, body):
    try:
        result = json.loads(body)
        print("=" * 60)
        print("Received result:")
        print(json.dumps(result, indent=2))
        print("=" * 60)

        workflow_id = result.get('workflow_id')
        task_id = result.get('task_id')
        status = result.get('status')

        if status in TERMINAL:
            try:
                persisted = _persist_terminal(result)
            except Exception as e:
                # Real Postgres error (connection/constraint) — requeue.
                print(f"Postgres persist failed for {task_id}: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                return

            if persisted and workflow_id and task_id:
                is_complete = add_task_completed(workflow_id, task_id)
                if is_complete:
                    _mark_workflow_complete(workflow_id)
                    print("=" * 60)
                    print(f"WORKFLOW {workflow_id} COMPLETE!")
                    print("=" * 60)

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag)


def main():
    print("Starting result consumer...")
    print(f"Connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}")
    PG_POOL.open()
    PG_POOL.wait()
    print(f"Connected to Postgres at {PG_CONNINFO}")

    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=RESULTS_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=RESULTS_QUEUE,
        on_message_callback=callback,
        auto_ack=False,
    )

    print(f"Waiting for results from queue '{RESULTS_QUEUE}'...")
    print("Press CTRL+C to exit")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nStopping consumer...")
        channel.stop_consuming()
        connection.close()
        PG_POOL.close()
        print("Consumer stopped.")


if __name__ == '__main__':
    main()
