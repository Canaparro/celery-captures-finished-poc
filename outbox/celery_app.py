import json
import logging
import hashlib
from datetime import datetime
from time import sleep

import pika
from celery import Celery, Task

from exceptions import PermanentFailureException, TransientFailureException
from models import Record
from outbox.redis_counter import add_task_created

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


def publish_result_to_queue(workflow_id: str, task_id: str, status: str, message: str):
    """Publish task result to RabbitMQ queue

    Args:
        workflow_id: The workflow ID
        task_id: The task ID
        status: Status of the task (success, failure, workflow_complete)
        message: Human-readable message
    """
    result = {
        'workflow_id': workflow_id,
        'task_id': task_id,
        'status': status,
        'message': message,
        'timestamp': datetime.now().isoformat()
    }

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


class CustomTask(Task):
    """Custom task class with on_success and on_failure callbacks"""

    def run(self, **kwargs):
        print("test")
        super().run(**kwargs)

    def on_success(self, retval, task_id, args, kwargs):
        workflow_id = args[0]
        record = Record.model_validate(args[1])

        publish_result_to_queue(
            workflow_id=workflow_id,
            task_id=task_id,
            status='success',
            message=f"Processed record {record.record_id}"
        )

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        workflow_id = args[0]
        record = Record.model_validate(args[1])

        if isinstance(exc, PermanentFailureException):
            publish_result_to_queue(
                workflow_id=workflow_id,
                task_id=task_id,
                status='permanent_failure',
                message=f"Permanent failure processing record {record.record_id}"
            )

        elif isinstance(exc, TransientFailureException):
            # Transient failure - retry
            publish_result_to_queue(
                workflow_id=workflow_id,
                task_id=task_id,
                status='transient_failure',
                message=f"Transient failure processing record {record.record_id}"
            )
            self.retry(countdown=2, max_retries=3)


@app.task(bind=True, base=CustomTask)
def process_record(self, workflow_id: str, record_as_dict: dict):
    """
    Simulates processing a record by calling an external API,
    processing its children, and publishing the result to a queue.
    """
    record = Record.model_validate(record_as_dict)
    if record.permanent_failure:
        raise PermanentFailureException

    if record.transient_failure:
        retry_count = self.request.retries
        if retry_count < 2:
            raise TransientFailureException

    records = call_external_api(record)
    process_children(self, workflow_id, records)

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
            task_id=task_id
        )
