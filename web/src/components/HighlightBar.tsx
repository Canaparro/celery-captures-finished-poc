import type { HighlightBy } from '../api';

interface Props {
  workflowId: string | null;
  current: HighlightBy | null;
  onPick: (by: HighlightBy | null) => void;
}

const OPTIONS: { key: HighlightBy; label: string }[] = [
  { key: 'slowest', label: 'Slowest' },
  { key: 'most_retries', label: 'Most retries' },
  { key: 'failed', label: 'Failed' },
];

export function HighlightBar({ workflowId, current, onPick }: Props) {
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
