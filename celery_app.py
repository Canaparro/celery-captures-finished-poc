import json
from datetime import datetime
from time import sleep

import pika
from celery import Celery

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


@app.task(bind=True)
def capture_post_comments(self, workflow_id, data):
    """Simulates capturing comments for a post"""
    comments = call_network_comments_api(self, workflow_id, data)

    # Publish result to RabbitMQ
    publish_result_to_queue(comments)


def call_network_comments_api(task, workflow_id, data):
    """Simulates calling a network comments API"""
    sleep(3)
    current_page = data["page"]
    total_pages = data["total_pages"]
    if current_page < total_pages:
        # create a new task with page = current_page + 1
        publish_new_task_delay(task, current_page, total_pages, workflow_id)
    return {"result": f"page {current_page}"}

def publish_new_task_delay(task, current_page, total_pages, workflow_id):
    capture_post_comments.delay(workflow_id, {
        "page": current_page + 1,
        "total_pages": total_pages,
    })

def publish_new_task_chord(task, current_page, total_pages, workflow_id):
    task.add_to_chord(
        capture_post_comments.s(workflow_id, {
            "page": current_page + 1,
            "total_pages": total_pages,
        })
    )


@app.task
def capture_finished(workflow_id):
    publish_result_to_queue(f"Workflow {workflow_id} finished at {datetime.now()}")
