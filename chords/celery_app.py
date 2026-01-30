import json
import logging
from datetime import datetime
from time import sleep

import pika
from celery import Celery, Task

from exceptions import PermanentFailureException, TransientFailureException
from models import Record

# Suppress pika logs
logging.getLogger('pika').setLevel(logging.WARNING)

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
    task_ignore_result=False,  # Store results in backend for chords
)

# RabbitMQ configuration for publishing results
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5682
RABBITMQ_USER = 'guest'
RABBITMQ_PASSWORD = 'guest'
RESULTS_QUEUE = 'task_results'


def publish_result_to_queue(result):
    """Publish task result to RabbitMQ queue"""
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
    """Custom task class with on_success callback"""

    def on_success(self, retval, task_id, args, kwargs):
        workflow_id = args[0]
        record = Record.model_validate(args[1])
        publish_result_to_queue(f"Processed record {record.record_id} in workflow {workflow_id} at {datetime.now()}")

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        workflow_id = args[0]
        record = Record.model_validate(args[1])
        if isinstance(exc, PermanentFailureException):
            publish_result_to_queue(f"Permanent failure processing record {record.record_id} in workflow {workflow_id} at {datetime.now()}")
        if isinstance(exc, TransientFailureException):
            self.retry(countdown=2, max_retries=3)


@app.task(bind=True, base=CustomTask)
def process_record(self, workflow_id: str, record_as_dict: dict):
    """
    Simulates processing a record by calling an external API,
    processing its children, and publishing the result to a queue.
    """
    retry_count = self.request.retries
    record = Record.model_validate(record_as_dict)
    if record.permanent_failure:
        raise PermanentFailureException

    if record.transient_failure and retry_count < 2:
        raise TransientFailureException

    records = call_external_api(record)
    process_children(self, workflow_id, records)

def call_external_api(record: Record):
    """Simulate an external API call that processes the record."""
    sleep(3)
    return record.children

def process_children(task, workflow_id: str, records: list[Record]):
    for child_record in records:
        publish_new_task_chord(task, workflow_id, child_record)


def publish_new_task_delay(_, workflow_id: str, record: Record):
    process_record.delay(workflow_id, record.model_dump())

def publish_new_task_chord(task, workflow_id: str, record: Record):
    new_task = process_record.s(workflow_id, record.model_dump())
    task.add_to_chord(new_task)


@app.task
def process_finished(workflow_id: str):
    publish_result_to_queue(f"Workflow {workflow_id} finished at {datetime.now()}")
