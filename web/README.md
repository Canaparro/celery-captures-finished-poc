# Web — React Flow viewer

Vite + React + TypeScript frontend for browsing the task hierarchy. Uses **[React Flow](https://reactflow.dev/)** (`@xyflow/react`) for the graph and **dagre** for tree layout.

## Files

```
src/
  main.tsx                  app entry; imports React Flow CSS
  App.tsx                   top-level: fetches data, builds the graph, collapse/expand state, highlight state
  api.ts                    tiny fetch wrappers around /api/*
  layout.ts                 dagre top-to-bottom tree layout
  types.ts                  TaskSummary, TaskDetail, TaskMetrics
  components/
    TaskNode.tsx            custom React Flow node for a task (record id, status color, retry/slow badges)
    WorkflowNode.tsx        custom root node showing aggregated stats (total tasks/retries/compute/wall time)
    DetailPanel.tsx         side pane shown on node click
    HighlightBar.tsx        top bar: Slowest / Most retries / Failed buttons
```

## Graph structure

- Single root: a synthetic **workflow** node whose id is the `workflow_id`.
- Every task has `parent_task_id` set (the first task's parent is the workflow id itself), so the graph always has exactly one root and tasks cleanly descend from the workflow.
- Tasks start **collapsed** (only the workflow + its direct children visible); clicking a node toggles collapse/expand.
- Clicking a task node also opens the detail pane with `last_message` and the full `metrics` block. Clicking the workflow node just collapses/expands; it has no DB row.

## Highlights

Clicking **Slowest / Most retries / Failed** hits `/api/workflows/{id}/highlights`. Matching nodes get a yellow outline; if any are inside a collapsed subtree, their ancestors auto-expand.

## Development

```bash
npm install
npm run dev      # Vite dev server on http://localhost:5173 with /api → 8000 proxy
```

Dev server requires the FastAPI backend on `:8000` (`poetry run uvicorn viewer.api:app --port 8000`).

## Production build

```bash
npm run build    # outputs to web/dist/
```

FastAPI then serves `web/dist/` at `/` automatically. After changing any frontend file, rebuild — there is no watch mode in production, the backend serves whatever is in `dist/`.

## Data shape expected from the API

```ts
interface TaskSummary {
  task_id: string;
  parent_task_id: string | null;   // null only if the row predates the workflow-as-root change
  record_id: number;
  final_status: 'success' | 'permanent_failure' | 'transient_failure_exhausted';
  metrics: {
    retry_count: number;
    total_duration_ms: number | null;
    first_seen_at: string | null;    // ISO-8601
    finished_at: string | null;      // ISO-8601
  };
}
```

## Customizing

- **Node visuals** — edit `TaskNode.tsx` / `WorkflowNode.tsx`. Both are plain React components.
- **Layout direction** — change `rankdir: 'TB'` in `layout.ts` to `'LR'` for left-to-right trees.
- **Aggregates on the workflow node** — `workflowSummary()` in `App.tsx` computes them from the tasks array; add more there.
- **Collapse default** — see the `setCollapsed(...)` call in the `useEffect` that loads tasks.
