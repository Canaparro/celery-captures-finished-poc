import type { TaskDetail } from '../types';

interface Props {
  task: TaskDetail | null;
  onClose: () => void;
}

export function DetailPanel({ task, onClose }: Props) {
  if (!task) return null;
  return (
    <div
      style={{
        position: 'fixed',
        top: 60,
        right: 0,
        bottom: 0,
        width: 420,
        background: '#1e1e1e',
        color: '#eee',
        borderLeft: '1px solid #333',
        padding: 16,
        boxSizing: 'border-box',
        overflowY: 'auto',
        fontFamily: 'system-ui, sans-serif',
        fontSize: 13,
        zIndex: 10,
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h2 style={{ margin: 0, fontSize: 16 }}>
          {task.label ?? `record #${task.record_id}`} — {task.final_status}
        </h2>
        <button
          onClick={onClose}
          style={{
            background: 'transparent',
            color: '#eee',
            border: '1px solid #444',
            borderRadius: 4,
            cursor: 'pointer',
            padding: '2px 8px',
          }}
        >
          ✕
        </button>
      </div>

      <h3 style={{ marginTop: 16, marginBottom: 4, fontSize: 13, color: '#aaa' }}>Result</h3>
      <div>{task.last_message}</div>

      <h3 style={{ marginTop: 16, marginBottom: 4, fontSize: 13, color: '#aaa' }}>Metrics</h3>
      <pre
        style={{
          background: '#111',
          padding: 10,
          borderRadius: 4,
          overflowX: 'auto',
          fontSize: 12,
        }}
      >
        {JSON.stringify(task.metrics, null, 2)}
      </pre>

      <h3 style={{ marginTop: 16, marginBottom: 4, fontSize: 13, color: '#aaa' }}>Identifiers</h3>
      <div style={{ fontFamily: 'monospace', fontSize: 11, wordBreak: 'break-all' }}>
        <div>record_id: {task.record_id}</div>
        <div>task_id: {task.task_id}</div>
        <div>parent_task_id: {task.parent_task_id ?? '(root)'}</div>
      </div>

      <h3 style={{ marginTop: 16, marginBottom: 4, fontSize: 13, color: '#aaa' }}>Timestamps</h3>
      <div style={{ fontFamily: 'monospace', fontSize: 11 }}>
        <div>created_at: {task.created_at}</div>
        <div>updated_at: {task.updated_at}</div>
      </div>
    </div>
  );
}
