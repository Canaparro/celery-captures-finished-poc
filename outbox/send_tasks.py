import argparse
import random
import uuid

from models import Record
from outbox.redis_counter import initialize_workflow, add_task_created, redis_client
from outbox.celery_app import process_record, generate_task_id


def build_random_tree(
    count: int,
    rng: random.Random,
    max_children: int,
    transient_rate: float,
    permanent_rate: float,
) -> Record:
    """Build a tree of exactly `count` Records. Each new record is attached to a
    uniformly-random existing record that still has room under `max_children`."""
    if count < 1:
        raise ValueError("count must be >= 1")

    def make(record_id: int) -> Record:
        roll = rng.random()
        transient = roll < transient_rate
        permanent = (not transient) and (roll < transient_rate + permanent_rate)
        return Record(
            record_id=record_id,
            transient_failure=transient,
            permanent_failure=permanent,
        )

    root = make(1)
    # Records that still have capacity for more children.
    attachable: list[Record] = [root]
    next_id = 2

    while next_id <= count:
        parent = rng.choice(attachable)
        child = make(next_id)
        parent.children.append(child)
        attachable.append(child)
        if len(parent.children) >= max_children:
            attachable.remove(parent)
        next_id += 1

    return root


def main():
    parser = argparse.ArgumentParser(description="Kick off a workflow with a random task tree.")
    parser.add_argument("--count", type=int, default=5, help="Total number of tasks in the tree (default: 5)")
    parser.add_argument("--seed", type=int, default=None, help="RNG seed for reproducibility (default: system random)")
    parser.add_argument("--max-children", type=int, default=4, help="Max children per node (default: 4)")
    parser.add_argument("--transient-failure-rate", type=float, default=0.15,
                        help="Probability each non-root node is marked transient_failure (default: 0.15)")
    parser.add_argument("--permanent-failure-rate", type=float, default=0.05,
                        help="Probability each non-root node is marked permanent_failure (default: 0.05)")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    root = build_random_tree(
        count=args.count,
        rng=rng,
        max_children=args.max_children,
        transient_rate=args.transient_failure_rate,
        permanent_rate=args.permanent_failure_rate,
    )

    print("=" * 60)
    print(f"Sending {args.count} tasks to Celery workers...")
    if args.seed is not None:
        print(f"(seed={args.seed})")
    print("=" * 60)

    workflow_id = str(uuid.uuid4())
    print(f"Starting workflow with ID: {workflow_id}")

    initialize_workflow(workflow_id)
    redis_client.set("workflow:current", workflow_id)

    task_id = generate_task_id(workflow_id, 'process_record', root.record_id)
    add_task_created(workflow_id, task_id)
    print(f"Added root task {task_id} to created set")

    process_record.apply_async(
        args=[workflow_id, root.model_dump()],
        kwargs={'parent_task_id': workflow_id},
        task_id=task_id,
    )

    print(f"Published initial task with ID: {task_id}")
    print("=" * 60)


if __name__ == '__main__':
    main()
