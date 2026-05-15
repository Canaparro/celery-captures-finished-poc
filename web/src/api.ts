import type { TaskDetail, TaskSummary } from './types';

async function json<T>(res: Response): Promise<T> {
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json() as Promise<T>;
}

export async function fetchCurrentWorkflow(): Promise<string | null> {
  const res = await fetch('/api/workflows/current');
  if (res.status === 404) return null;
  const data = await json<{ workflow_id: string }>(res);
  return data.workflow_id;
}

export async function fetchTasks(workflowId: string): Promise<TaskSummary[]> {
  return json(await fetch(`/api/workflows/${workflowId}/tasks`));
}

export async function fetchTaskDetail(
  workflowId: string,
  taskId: string,
): Promise<TaskDetail> {
  return json(await fetch(`/api/workflows/${workflowId}/tasks/${taskId}`));
}

export type HighlightBy = 'slowest' | 'most_retries' | 'failed';

export async function fetchHighlights(
  workflowId: string,
  by: HighlightBy,
): Promise<string[]> {
  const res = await fetch(
    `/api/workflows/${workflowId}/highlights?by=${by}&limit=50`,
  );
  const data = await json<{ task_ids: string[] }>(res);
  return data.task_ids;
}
