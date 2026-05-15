import { Handle, Position } from '@xyflow/react';
import type { NodeProps } from '@xyflow/react';
import type { FinalStatus, TaskMetrics } from '../types';

export interface TaskNodeData {
  task_id: string;
  record_id: number;
  final_status: FinalStatus;
  metrics: TaskMetrics;
  highlighted: boolean;
  collapsed: boolean;
  hasChildren: boolean;
  failed_descendant_ids: string[];
  onRevealFailures?: (taskId: string) => void;
  [key: string]: unknown;
}

const STATUS_COLOR: Record<FinalStatus, string> = {
  success: '#2e7d32',
  permanent_failure: '#c62828',
  transient_failure_exhausted: '#ef6c00',
};

const SLOW_MS_THRESHOLD = 5000;

export function TaskNode({ data }: NodeProps) {
  const d = data as TaskNodeData;
  const bg = STATUS_COLOR[d.final_status];
  const retries = d.metrics.retry_count ?? 0;
  const durationMs = d.metrics.total_duration_ms;
  const isSlow = durationMs != null && durationMs >= SLOW_MS_THRESHOLD;

  return (
    <div
      style={{
        background: bg,
        color: '#fff',
        borderRadius: 8,
        padding: '8px 12px',
        width: 200,
        height: 70,
        boxSizing: 'border-box',
        boxShadow: d.highlighted ? '0 0 0 3px #ffeb3b' : '0 1px 3px rgba(0,0,0,0.3)',
        fontFamily: 'system-ui, sans-serif',
        cursor: 'pointer',
        display: 'flex',
        flexDirection: 'column',
        justifyContent: 'space-between',
      }}
    >
      <Handle type="target" position={Position.Top} style={{ background: '#555' }} />
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span style={{ fontSize: 18, fontWeight: 600 }}>record #{d.record_id}</span>
        {d.hasChildren && (
          <span style={{ fontSize: 12, opacity: 0.8 }}>
            {d.collapsed ? '▸ expand' : '▾ collapse'}
          </span>
        )}
      </div>
      <div style={{ display: 'flex', gap: 6, fontSize: 11, flexWrap: 'wrap' }}>
        <span style={{ background: 'rgba(0,0,0,0.25)', padding: '1px 6px', borderRadius: 4 }}>
          {d.final_status}
        </span>
        {retries > 0 && (
          <span style={{ background: '#fbc02d', color: '#222', padding: '1px 6px', borderRadius: 4 }}>
            {retries} retries
          </span>
        )}
        {isSlow && (
          <span style={{ background: '#0288d1', padding: '1px 6px', borderRadius: 4 }}>
            slow ({Math.round((durationMs ?? 0) / 1000)}s)
          </span>
        )}
        {d.failed_descendant_ids.length > 0 && d.final_status === 'success' && (
          <span
            onClick={(e) => {
              e.stopPropagation();
              d.onRevealFailures?.(d.task_id);
            }}
            style={{
              background: '#c62828',
              padding: '1px 6px',
              borderRadius: 4,
              cursor: 'pointer',
            }}
            title={`Click to reveal ${d.failed_descendant_ids.length} failure(s) downstream`}
          >
            ⚠ {d.failed_descendant_ids.length} failure{d.failed_descendant_ids.length > 1 ? 's' : ''} downstream
          </span>
        )}
      </div>
      <Handle type="source" position={Position.Bottom} style={{ background: '#555' }} />
    </div>
  );
}
