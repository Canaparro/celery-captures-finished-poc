import json
import logging
import hashlib
from datetime import datetime, timezone
from time import sleep

import pika
from celery import Celery

from exceptions import PermanentFailureException, TransientFailureException
from models import Record
from outbox.redis_counter import add_task_created, redis_client

# Suppress pika logs
logging.getLogger('pika').setLevel(logging.WARNING)


def generate_task_id(workflow_id: str, task_name: str, record_id: int) -> str:
    """
    Generate a deterministic task ID based on workflow, task name, and record ID.
    This ensures the same task with same parameters always gets the same ID.

    Args:
        workflow_id: The workflow ID
        task_name: Name of the task (e.g., 'process_record')
        record_id: The record ID being processed

    Returns:
        A deterministic task ID
    """
    # Create a string combining all parameters
    task_string = f"{workflow_id}:{task_name}:{record_id}"

    # Generate SHA256 hash
    hash_digest = hashlib.sha256(task_string.encode()).hexdigest()

    # Use first 32 characters for task ID (same format as Celery UUIDs)
    return f"{hash_digest[:8]}-{hash_digest[8:12]}-{hash_digest[12:16]}-{hash_digest[16:20]}-{hash_digest[20:32]}"

# Configure Celery with RabbitMQ broker and Redis backend
app = Celery(
    'tasks',
    broker='amqp://guest:guest@localhost:5682//',
    backend='redis://localhost:6389/0'
)

# Configure Celery settings
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_ignore_result=False,
)

# RabbitMQ configuration for publishing results
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5682
RABBITMQ_USER = 'guest'
RABBITMQ_PASSWORD = 'guest'
RESULTS_QUEUE = 'task_results'


def publish_result_to_queue(
    workflow_id: str,
    task_id: str,
    status: str,
    message: str,
    *,
    parent_task_id: str | None,
    record_id: int,
    label: str | None,
    attempt: int,
    first_seen_at: str,
):
    """Publish task result to RabbitMQ queue with retry logic."""
    result = {
        'workflow_id': workflow_id,
        'task_id': task_id,
        'parent_task_id': parent_task_id,
        'record_id': record_id,
        'label': label,
        'status': status,
        'message': message,
        'attempt': attempt,
        'first_seen_at': first_seen_at,
        'timestamp': datetime.now(timezone.utc).isoformat(),
    }

    max_attempts = 3
    retry_delay = 1

    for attempt in range(max_attempts):
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=credentials
            )

            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            # Declare the results queue
            channel.queue_declare(queue=RESULTS_QUEUE, durable=True)

            # Publish the result
            channel.basic_publish(
                exchange='',
                routing_key=RESULTS_QUEUE,
                body=json.dumps(result),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    content_type='application/json'
                )
            )

            connection.close()
            print(f"Published result to queue: {result}")
            return  # Success, exit function

        except Exception as e:
            print(f"Failed to publish result (attempt {attempt + 1}/{max_attempts}): {e}")
            if attempt < max_attempts - 1:
                sleep(retry_delay)
            else:
                # All attempts failed, re-raise the exception
                raise


def _capture_first_seen_at(task_id: str) -> str:
    """Return the stable first-receive timestamp for this task_id.

    Uses Redis SETNX so the anchor survives in-process retries (and any future
    cross-invocation retry mechanism). 24h TTL so stale keys don't accumulate.
    """
    key = f"task:{task_id}:first_seen_at"
    now_iso = datetime.now(timezone.utc).isoformat()
    if redis_client.set(key, now_iso, nx=True, ex=86400):
        return now_iso
    existing = redis_client.get(key)
    return existing or now_iso


@app.task(bind=True)
def process_record(self, workflow_id: str, record_as_dict: dict, parent_task_id: str | None = None):
    """
    Simulates processing a record by calling an external API,
    processing its children, and publishing the result to a queue.
    """
    record = Record.model_validate(record_as_dict)
    max_retries = 3
    retry_delay = 2
    first_seen_at = _capture_first_seen_at(self.request.id)

    common = {
        'workflow_id': workflow_id,
        'task_id': self.request.id,
        'parent_task_id': parent_task_id,
        'record_id': record.record_id,
        'label': record.label,
        'first_seen_at': first_seen_at,
    }

    for attempt in range(max_retries):
        try:
            if record.permanent_failure:
                raise PermanentFailureException

            if record.transient_failure and attempt < 2:
                raise TransientFailureException

            records = call_external_api(record)
            process_children(self, workflow_id, records)

            publish_result_to_queue(
                **common,
                status='success',
                message=f"Processed record {record.record_id}",
                attempt=attempt,
            )
            return

        except PermanentFailureException:
            publish_result_to_queue(
                **common,
                status='permanent_failure',
                message=f"Permanent failure processing record {record.record_id}",
                attempt=attempt,
            )
            return

        except TransientFailureException:
            publish_result_to_queue(
                **common,
                status='transient_failure',
                message=f"Transient failure processing record {record.record_id}, attempt {attempt + 1}/{max_retries}",
                attempt=attempt,
            )

            if attempt < max_retries - 1:
                sleep(retry_delay)
            else:
                publish_result_to_queue(
                    **common,
                    status='transient_failure_exhausted',
                    message=f"Transient failure processing record {record.record_id} - retries exhausted",
                    attempt=attempt,
                )
                return

def call_external_api(record: Record):
    """Simulate an external API call that processes the record."""
    sleep(3)
    return record.children

def process_children(task, workflow_id: str, records: list[Record]):
    """Spawn child tasks and add them to the created set"""
    if not records:
        return

    for child_record in records:
        task_id = generate_task_id(workflow_id, task.name, child_record.record_id)

        add_task_created(workflow_id, task_id)

        process_record.apply_async(
            args=[workflow_id, child_record.model_dump()],
            kwargs={'parent_task_id': task.request.id},
            task_id=task_id
        )
