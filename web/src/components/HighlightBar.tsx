import type { HighlightBy, WorkflowStatus } from '../api';

interface Props {
  workflowId: string | null;
  status: WorkflowStatus | null;
  current: HighlightBy | null;
  onPick: (by: HighlightBy | null) => void;
}

const OPTIONS: { key: HighlightBy; label: string }[] = [
  { key: 'slowest', label: 'Slowest' },
  { key: 'most_retries', label: 'Most retries' },
  { key: 'failed', label: 'Failed' },
];

function StatusPill({ status }: { status: WorkflowStatus | null }) {
  if (!status) {
    return (
      <span style={pill({ bg: '#333', fg: '#888' })}>status: …</span>
    );
  }
  if (status.status === 'pending') {
    return (
      <span
        style={pill({ bg: '#fbc02d', fg: '#222' })}
        title={`${status.completed}/${status.created} tasks complete`}
      >
        ⏳ pending · {status.completed}/{status.created}
      </span>
    );
  }
  if (status.status === 'complete') {
    return (
      <span
        style={pill({ bg: '#2e7d32', fg: '#fff' })}
        title={`${status.completed}/${status.created} tasks complete`}
      >
        ✓ complete · {status.completed}/{status.created}
      </span>
    );
  }
  return (
    <span
      style={pill({ bg: '#444', fg: '#aaa' })}
      title="Workflow tracking not initialized in Redis"
    >
      ? unknown
    </span>
  );
}

function pill({ bg, fg }: { bg: string; fg: string }): React.CSSProperties {
  return {
    background: bg,
    color: fg,
    border: '1px solid rgba(0,0,0,0.25)',
    borderRadius: 999,
    padding: '3px 10px',
    fontSize: 11,
    fontWeight: 600,
    letterSpacing: 0.3,
    marginRight: 8,
  };
}

export function HighlightBar({ workflowId, status, current, onPick }: Props) {
  return (
    <div
      style={{
        position: 'fixed',
        top: 0,
        left: 0,
        right: 0,
        height: 48,
        background: '#111',
        color: '#eee',
        borderBottom: '1px solid #333',
        display: 'flex',
        alignItems: 'center',
        gap: 8,
        padding: '0 16px',
        fontFamily: 'system-ui, sans-serif',
        fontSize: 13,
        zIndex: 20,
      }}
    >
      <strong style={{ marginRight: 8 }}>Celery task viewer</strong>
      <StatusPill status={status} />
      <span style={{ color: '#888', fontSize: 11, marginRight: 16 }}>
        workflow: {workflowId ?? '—'}
      </span>
      <span style={{ color: '#888' }}>Highlight:</span>
      {OPTIONS.map((o) => (
        <button
          key={o.key}
          onClick={() => onPick(current === o.key ? null : o.key)}
          style={{
            background: current === o.key ? '#ffeb3b' : '#222',
            color: current === o.key ? '#111' : '#eee',
            border: '1px solid #444',
            borderRadius: 4,
            padding: '4px 10px',
            cursor: 'pointer',
            fontSize: 12,
          }}
        >
          {o.label}
        </button>
      ))}
      {current && (
        <button
          onClick={() => onPick(null)}
          style={{
            marginLeft: 'auto',
            background: 'transparent',
            color: '#aaa',
            border: '1px solid #444',
            borderRadius: 4,
            padding: '4px 10px',
            cursor: 'pointer',
            fontSize: 12,
          }}
        >
          Clear highlight
        </button>
      )}
    </div>
  );
}
