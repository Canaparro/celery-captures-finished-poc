import { Handle, Position } from '@xyflow/react';
import type { NodeProps } from '@xyflow/react';

export interface WorkflowNodeData {
  workflow_id: string;
  task_count: number;
  success_count: number;
  failed_count: number;
  tasks_with_retries: number;
  total_retries: number;
  total_duration_ms: number;
  wall_time_ms: number | null;
  collapsed: boolean;
  hasChildren: boolean;
  [key: string]: unknown;
}

export function WorkflowNode({ data }: NodeProps) {
  const d = data as WorkflowNodeData;
  return (
    <div
      style={{
        background: '#1565c0',
        color: '#fff',
        borderRadius: 8,
        padding: '10px 14px',
        width: 260,
        boxSizing: 'border-box',
        boxShadow: '0 2px 6px rgba(0,0,0,0.4)',
        fontFamily: 'system-ui, sans-serif',
        cursor: 'pointer',
        border: '2px solid #90caf9',
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span style={{ fontSize: 12, letterSpacing: 1, opacity: 0.8 }}>WORKFLOW</span>
        {d.hasChildren && (
          <span style={{ fontSize: 11, opacity: 0.8 }}>
            {d.collapsed ? '▸ expand' : '▾ collapse'}
          </span>
        )}
      </div>
      <div
        style={{
          fontFamily: 'monospace',
          fontSize: 12,
          marginTop: 2,
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
        }}
        title={d.workflow_id}
      >
        {d.workflow_id}
      </div>
      <div style={{ display: 'flex', gap: 6, fontSize: 11, marginTop: 8, flexWrap: 'wrap' }}>
        <span style={{ background: 'rgba(0,0,0,0.25)', padding: '1px 6px', borderRadius: 4 }}>
          {d.task_count} tasks
        </span>
        <span style={{ background: '#2e7d32', padding: '1px 6px', borderRadius: 4 }}>
          {d.success_count} ok
        </span>
        {d.failed_count > 0 && (
          <span style={{ background: '#c62828', padding: '1px 6px', borderRadius: 4 }}>
            {d.failed_count} failed
          </span>
        )}
        {d.total_retries > 0 && (
          <span
            style={{ background: '#fbc02d', color: '#222', padding: '1px 6px', borderRadius: 4 }}
            title={`${d.tasks_with_retries} task(s) retried`}
          >
            {d.total_retries} retries
          </span>
        )}
      </div>
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '1fr 1fr',
          gap: 4,
          marginTop: 8,
          fontSize: 11,
          color: '#e3f2fd',
        }}
      >
        <div>
          <div style={{ opacity: 0.7 }}>compute</div>
          <div style={{ fontVariantNumeric: 'tabular-nums' }}>
            {(d.total_duration_ms / 1000).toFixed(1)}s
          </div>
        </div>
        <div>
          <div style={{ opacity: 0.7 }}>wall time</div>
          <div style={{ fontVariantNumeric: 'tabular-nums' }}>
            {d.wall_time_ms != null ? `${(d.wall_time_ms / 1000).toFixed(1)}s` : '—'}
          </div>
        </div>
      </div>
      <Handle type="source" position={Position.Bottom} style={{ background: '#90caf9' }} />
    </div>
  );
}
