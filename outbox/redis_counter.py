import logging
import redis

# Redis connection
redis_client = redis.Redis(host='localhost', port=6389, db=0, decode_responses=True)

# Lua script to add task to completed set and check if workflow is complete
# Returns 1 if workflow is complete (created == completed), 0 otherwise
CHECK_COMPLETION_SCRIPT = """
local created_key = KEYS[1]
local completed_key = KEYS[2]
local task_id = ARGV[1]

-- Add task to completed set
redis.call('SADD', completed_key, task_id)

-- Check if sets are equal by comparing sizes and using SDIFF
local created_size = redis.call('SCARD', created_key)
local completed_size = redis.call('SCARD', completed_key)

-- If sizes don't match, not complete
if created_size ~= completed_size then
    return 0
end

-- If sizes match, check if created - completed is empty
local diff = redis.call('SDIFF', created_key, completed_key)
if #diff == 0 then
    return 1  -- Sets are equal, workflow complete
else
    return 0
end
"""


def initialize_workflow(workflow_id: str):
    """
    Initialize the workflow tracking sets.

    Args:
        workflow_id: The workflow ID
    """
    created_key = f"workflow:{workflow_id}:created"
    completed_key = f"workflow:{workflow_id}:completed"

    # Delete any existing sets (in case of retry)
    redis_client.delete(created_key)
    redis_client.delete(completed_key)

    logging.info(f"Initialized workflow {workflow_id}")


def add_task_created(workflow_id: str, task_id: str):
    """
    Add a task ID to the created set when a task is spawned.

    Args:
        workflow_id: The workflow ID
        task_id: The Celery task ID
    """
    created_key = f"workflow:{workflow_id}:created"
    redis_client.sadd(created_key, task_id)
    logging.info(f"Added task {task_id} to created set for workflow {workflow_id}")


def add_task_completed(workflow_id: str, task_id: str) -> bool:
    """
    Add a task ID to the completed set when a task finishes.
    Returns True if this was the last task and workflow is complete.

    Args:
        workflow_id: The workflow ID
        task_id: The Celery task ID

    Returns:
        True if workflow is complete, False otherwise
    """
    created_key = f"workflow:{workflow_id}:created"
    completed_key = f"workflow:{workflow_id}:completed"

    # Use Lua script to atomically add and check completion
    is_complete = redis_client.eval(
        CHECK_COMPLETION_SCRIPT,
        2,
        created_key,
        completed_key,
        task_id
    )

    logging.info(f"Added task {task_id} to completed set for workflow {workflow_id}, complete: {is_complete}")
    return bool(is_complete)


def get_pending_tasks(workflow_id: str) -> set:
    """
    Get the set of pending task IDs (created but not completed).

    Args:
        workflow_id: The workflow ID

    Returns:
        Set of pending task IDs
    """
    created_key = f"workflow:{workflow_id}:created"
    completed_key = f"workflow:{workflow_id}:completed"

    # Use SDIFF to get created - completed
    pending = redis_client.sdiff(created_key, completed_key)
    return pending


def get_workflow_stats(workflow_id: str) -> dict:
    """
    Get statistics about the workflow.

    Args:
        workflow_id: The workflow ID

    Returns:
        Dict with created_count, completed_count, pending_count
    """
    created_key = f"workflow:{workflow_id}:created"
    completed_key = f"workflow:{workflow_id}:completed"

    created_count = redis_client.scard(created_key)
    completed_count = redis_client.scard(completed_key)

    return {
        'created_count': created_count,
        'completed_count': completed_count,
        'pending_count': created_count - completed_count
    }
