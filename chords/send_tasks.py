import uuid

from celery import chord

from .celery_app import process_record, process_finished
from models import Record


def main():
    """Send various tasks to Celery workers"""

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
                # transient_failure=True,
            )
        ],
    )

    workflow_id = str(uuid.uuid4())
    print(f"Starting workflow with ID: {workflow_id}")
    result = chord(header=[process_record.s(workflow_id, record.model_dump())], body=process_finished.si(workflow_id)).apply_async()
    print(f"Initial task ID: {result.id}")
    print("=" * 60)


if __name__ == '__main__':
    main()
