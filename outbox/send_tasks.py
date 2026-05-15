import uuid
from itertools import count
from typing import Iterator

from models import Record
from outbox.redis_counter import initialize_workflow, add_task_created, redis_client
from outbox.celery_app import process_record, generate_task_id

SECTIONS = [("timeline", 5), ("album", 3), ("videos", 1)]
TRANSIENT_FAILURES = {("album", 2)}      # (section, page_number)
PERMANENT_FAILURES = {("timeline", 4)}


def build_chain(section_name: str, page_count: int, ids: Iterator[int]) -> Record:
    """Build a section node and a chain of `page_count` page records below it.

    The chain is constructed bottom-up: each page becomes the single child of
    the page before it. The returned Record is the section, with the chain
    head as its only child.
    """
    head: Record | None = None
    parent: Record | None = None
    for page_number in range(1, page_count + 1):
        page = Record(
            record_id=next(ids),
            label=f"page {page_number}",
            transient_failure=(section_name, page_number) in TRANSIENT_FAILURES,
            permanent_failure=(section_name, page_number) in PERMANENT_FAILURES,
        )
        if parent is None:
            head = page
        else:
            parent.children.append(page)
        parent = page

    return Record(
        record_id=next(ids),
        label=section_name,
        children=[head] if head else [],
    )


def build_profile_tree() -> Record:
    ids = count(1)
    root = Record(record_id=next(ids), label="Facebook page")
    for name, page_count in SECTIONS:
        root.children.append(build_chain(name, page_count, ids))
    return root


def main():
    root = build_profile_tree()
    workflow_id = str(uuid.uuid4())

    print("=" * 60)
    print(f"Starting workflow with ID: {workflow_id}")
    print("Shape: start → [timeline×5, album×3, videos×1] (page chains)")
    print("=" * 60)

    initialize_workflow(workflow_id)
    redis_client.set("workflow:current", workflow_id)

    task_id = generate_task_id(workflow_id, "process_record", root.record_id)
    add_task_created(workflow_id, task_id)
    process_record.apply_async(
        args=[workflow_id, root.model_dump()],
        kwargs={"parent_task_id": workflow_id},
        task_id=task_id,
    )
    print(f"Published initial task with ID: {task_id}")


if __name__ == "__main__":
    main()
