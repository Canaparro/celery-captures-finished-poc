export type FinalStatus =
  | 'success'
  | 'permanent_failure'
  | 'transient_failure_exhausted';

export interface TaskMetrics {
  retry_count: number;
  total_duration_ms: number | null;
  first_seen_at: string | null;
  finished_at: string | null;
}

export interface TaskSummary {
  task_id: string;
  parent_task_id: string | null;
  record_id: number;
  label: string | null;
  final_status: FinalStatus;
  metrics: TaskMetrics;
}

export interface TaskDetail extends TaskSummary {
  last_message: string | null;
  created_at: string;
  updated_at: string;
}
