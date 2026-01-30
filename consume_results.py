import json
import pika

# RabbitMQ configuration
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5682
RABBITMQ_USER = 'guest'
RABBITMQ_PASSWORD = 'guest'
RESULTS_QUEUE = 'task_results'


def callback(ch, method, properties, body):
    """Callback function to process messages from the queue"""
    try:
        result = json.loads(body)
        print("=" * 60)
        print("Received result:")
        print(json.dumps(result, indent=2))
        print("=" * 60)

        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error processing message: {e}")
        # Negative acknowledgement - message will be requeued
        ch.basic_nack(delivery_tag=method.delivery_tag)


def main():
    """Start consuming messages from the results queue"""
    print("Starting result consumer...")
    print(f"Connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}")

    # Set up connection
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # Declare the queue (idempotent - will not create if exists)
    channel.queue_declare(queue=RESULTS_QUEUE, durable=True)

    # Set prefetch count to 1 to distribute work evenly
    channel.basic_qos(prefetch_count=1)

    # Start consuming
    channel.basic_consume(
        queue=RESULTS_QUEUE,
        on_message_callback=callback,
        auto_ack=False  # Manual acknowledgement
    )

    print(f"Waiting for results from queue '{RESULTS_QUEUE}'...")
    print("Press CTRL+C to exit")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nStopping consumer...")
        channel.stop_consuming()
        connection.close()
        print("Consumer stopped.")


if __name__ == '__main__':
    main()