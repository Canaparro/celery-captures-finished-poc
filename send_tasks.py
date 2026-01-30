from celery import chord

from celery_app import capture_post_comments, capture_finished


def main():
    """Send various tasks to Celery workers"""

    print("=" * 60)
    print("Sending tasks to Celery workers...")
    print("=" * 60)

    data = {
        "page": 1,
        "total_pages": 3,
    }
    # result = capture_post_comments.delay(data)

    workflow_id = "workflow_12345"
    result = chord([capture_post_comments.s(workflow_id, data)], capture_finished.si(workflow_id)).apply_async()
    print(f"Initial task ID: {result.id}")
    print("=" * 60)


if __name__ == '__main__':
    main()
