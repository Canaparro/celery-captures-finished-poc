import uuid

from models import Record
from outbox.redis_counter import initialize_workflow, add_task_created
from outbox.celery_app import process_record, generate_task_id


def main():
    """Send tasks to Celery workers"""

    print("=" * 60)
    print("Sending tasks to Celery workers...")
    print("=" * 60)

    record = Record(
        record_id=1,
        children=[
            Record(
                record_id=3,
                children=[
                    Record(record_id=4),
                    Record(record_id=5),
                ]
            ),
            Record(
                record_id=2,
                transient_failure=True,
            )
        ],
    )

    workflow_id = str(uuid.uuid4())
    print(f"Starting workflow with ID: {workflow_id}")

    # Initialize the workflow (clears any existing sets)
    initialize_workflow(workflow_id)

    # Generate deterministic task ID for the first task
    task_id = generate_task_id(workflow_id, 'process_record', record.record_id)

    # Add to created set FIRST
    add_task_created(workflow_id, task_id)
    print(f"Added task {task_id} to created set")

    # Then publish the first task to Celery with specific task ID
    process_record.apply_async(
        args=[workflow_id, record.model_dump()],
        task_id=task_id
    )

    print(f"Published initial task with ID: {task_id}")
    print("=" * 60)


if __name__ == '__main__':
    main()
