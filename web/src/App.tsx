import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  type Edge,
  type Node,
  type NodeMouseHandler,
} from '@xyflow/react';

import {
  fetchCurrentWorkflow,
  fetchHighlights,
  fetchTaskDetail,
  fetchTasks,
  type HighlightBy,
} from './api';
import { layoutTree } from './layout';
import { TaskNode, type TaskNodeData } from './components/TaskNode';
import { WorkflowNode, type WorkflowNodeData } from './components/WorkflowNode';
import { DetailPanel } from './components/DetailPanel';
import { HighlightBar } from './components/HighlightBar';
import type { TaskDetail, TaskSummary } from './types';

const nodeTypes = { task: TaskNode, workflow: WorkflowNode };

function workflowSummary(
  workflowId: string,
  tasks: TaskSummary[],
  hasChildren: boolean,
): WorkflowNodeData {
  let success = 0;
  let failed = 0;
  let tasksWithRetries = 0;
  let totalRetries = 0;
  let totalDuration = 0;
  let minFirstSeen = Infinity;
  let maxFinished = -Infinity;

  for (const t of tasks) {
    if (t.final_status === 'success') success++;
    else failed++;

    const r = t.metrics.retry_count ?? 0;
    totalRetries += r;
    if (r > 0) tasksWithRetries++;

    const d = t.metrics.total_duration_ms ?? 0;
    totalDuration += d;

    if (t.metrics.first_seen_at) {
      const fs = Date.parse(t.metrics.first_seen_at);
      if (!Number.isNaN(fs) && fs < minFirstSeen) minFirstSeen = fs;
    }
    if (t.metrics.finished_at) {
      const fn = Date.parse(t.metrics.finished_at);
      if (!Number.isNaN(fn) && fn > maxFinished) maxFinished = fn;
    }
  }

  const wallTime =
    minFirstSeen !== Infinity && maxFinished !== -Infinity
      ? maxFinished - minFirstSeen
      : null;

  return {
    workflow_id: workflowId,
    task_count: tasks.length,
    success_count: success,
    failed_count: failed,
    tasks_with_retries: tasksWithRetries,
    total_retries: totalRetries,
    total_duration_ms: totalDuration,
    wall_time_ms: wallTime,
    collapsed: false,
    hasChildren,
  };
}

function buildGraph(
  workflowId: string,
  tasks: TaskSummary[],
): { nodes: Node[]; edges: Edge[] } {
  const childrenOf = new Map<string, string[]>();
  for (const t of tasks) {
    const parent = t.parent_task_id ?? workflowId;
    const arr = childrenOf.get(parent) ?? [];
    arr.push(t.task_id);
    childrenOf.set(parent, arr);
  }

  const workflowChildren = childrenOf.get(workflowId) ?? [];

  // Collect every failed task id under each task's subtree.
  const failedDescendants = new Map<string, string[]>();
  const taskById = new Map(tasks.map((t) => [t.task_id, t]));
  function visit(taskId: string): string[] {
    const acc: string[] = [];
    for (const child of childrenOf.get(taskId) ?? []) {
      const childTask = taskById.get(child);
      if (childTask && childTask.final_status !== 'success') acc.push(child);
      acc.push(...visit(child));
    }
    failedDescendants.set(taskId, acc);
    return acc;
  }
  for (const rootTaskId of workflowChildren) visit(rootTaskId);

  const nodes: Node[] = [
    {
      id: workflowId,
      type: 'workflow',
      position: { x: 0, y: 0 },
      data: workflowSummary(workflowId, tasks, workflowChildren.length > 0),
    },
    ...tasks.map((t) => ({
      id: t.task_id,
      type: 'task',
      position: { x: 0, y: 0 },
      data: {
        task_id: t.task_id,
        record_id: t.record_id,
        final_status: t.final_status,
        metrics: t.metrics,
        highlighted: false,
        collapsed: false,
        hasChildren: (childrenOf.get(t.task_id)?.length ?? 0) > 0,
        failed_descendant_ids: failedDescendants.get(t.task_id) ?? [],
      } satisfies TaskNodeData,
    })),
  ];

  const edges: Edge[] = tasks.map((t) => {
    const source = t.parent_task_id ?? workflowId;
    return {
      id: `${source}->${t.task_id}`,
      source,
      target: t.task_id,
      animated: false,
    };
  });

  return { nodes, edges };
}

function computeVisible(
  workflowId: string,
  tasks: TaskSummary[],
  collapsed: Set<string>,
): Set<string> {
  const childrenOf = new Map<string, string[]>();
  for (const t of tasks) {
    const parent = t.parent_task_id ?? workflowId;
    const arr = childrenOf.get(parent) ?? [];
    arr.push(t.task_id);
    childrenOf.set(parent, arr);
  }

  const visible = new Set<string>([workflowId]);
  const stack = [workflowId];
  while (stack.length) {
    const cur = stack.pop()!;
    if (collapsed.has(cur)) continue;
    for (const child of childrenOf.get(cur) ?? []) {
      visible.add(child);
      stack.push(child);
    }
  }
  return visible;
}

function ancestorsOf(
  workflowId: string,
  tasks: TaskSummary[],
  taskId: string,
): string[] {
  const parentOf = new Map<string, string>();
  for (const t of tasks) parentOf.set(t.task_id, t.parent_task_id ?? workflowId);
  const out: string[] = [];
  let cur: string | undefined = parentOf.get(taskId);
  while (cur) {
    out.push(cur);
    cur = parentOf.get(cur);
  }
  return out;
}

export function App() {
  const [workflowId, setWorkflowId] = useState<string | null>(null);
  const [tasks, setTasks] = useState<TaskSummary[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set());
  const [selected, setSelected] = useState<TaskDetail | null>(null);
  const [highlightBy, setHighlightBy] = useState<HighlightBy | null>(null);
  const [highlightIds, setHighlightIds] = useState<Set<string>>(new Set());

  useEffect(() => {
    fetchCurrentWorkflow()
      .then(setWorkflowId)
      .catch((e) => setError(String(e)));
  }, []);

  useEffect(() => {
    if (!workflowId) return;
    fetchTasks(workflowId)
      .then((ts) => {
        setTasks(ts);
        // Start with workflow expanded, every task collapsed.
        setCollapsed(new Set(ts.map((t) => t.task_id)));
      })
      .catch((e) => setError(String(e)));
  }, [workflowId]);

  const base = useMemo(
    () => (workflowId && tasks ? buildGraph(workflowId, tasks) : null),
    [workflowId, tasks],
  );
  const laidOut = useMemo(
    () => (base ? { ...base, nodes: layoutTree(base.nodes, base.edges) } : null),
    [base],
  );

  const visibleIds = useMemo(
    () =>
      workflowId && tasks
        ? computeVisible(workflowId, tasks, collapsed)
        : new Set<string>(),
    [workflowId, tasks, collapsed],
  );

  const onRevealFailures = useCallback(
    (fromTaskId: string) => {
      if (!workflowId || !tasks) return;
      const parentOf = new Map<string, string>();
      for (const t of tasks) parentOf.set(t.task_id, t.parent_task_id ?? workflowId);

      const childrenOf = new Map<string, string[]>();
      for (const t of tasks) {
        const parent = t.parent_task_id ?? workflowId;
        const arr = childrenOf.get(parent) ?? [];
        arr.push(t.task_id);
        childrenOf.set(parent, arr);
      }
      const failed: string[] = [];
      const stack = [...(childrenOf.get(fromTaskId) ?? [])];
      const taskById = new Map(tasks.map((t) => [t.task_id, t]));
      while (stack.length) {
        const id = stack.pop()!;
        const t = taskById.get(id);
        if (t && t.final_status !== 'success') failed.push(id);
        stack.push(...(childrenOf.get(id) ?? []));
      }

      setCollapsed((prev) => {
        const next = new Set(prev);
        next.delete(fromTaskId);
        for (const failId of failed) {
          let cur: string | undefined = parentOf.get(failId);
          while (cur && cur !== fromTaskId && cur !== workflowId) {
            next.delete(cur);
            cur = parentOf.get(cur);
          }
        }
        return next;
      });
    },
    [workflowId, tasks],
  );

  const nodes: Node[] = useMemo(() => {
    if (!laidOut) return [];
    return laidOut.nodes
      .filter((n) => visibleIds.has(n.id))
      .map((n) => {
        if (n.type === 'workflow') {
          const d = n.data as WorkflowNodeData;
          return {
            ...n,
            data: { ...d, collapsed: collapsed.has(n.id) } satisfies WorkflowNodeData,
          };
        }
        const d = n.data as TaskNodeData;
        return {
          ...n,
          data: {
            ...d,
            highlighted: highlightIds.has(n.id),
            collapsed: collapsed.has(n.id),
            onRevealFailures,
          } satisfies TaskNodeData,
        };
      });
  }, [laidOut, visibleIds, collapsed, highlightIds, onRevealFailures]);

  const edges: Edge[] = useMemo(() => {
    if (!laidOut) return [];
    return laidOut.edges.filter(
      (e) => visibleIds.has(e.source) && visibleIds.has(e.target),
    );
  }, [laidOut, visibleIds]);

  const onNodeClick = useCallback<NodeMouseHandler>(
    (_evt, node) => {
      setCollapsed((prev) => {
        const next = new Set(prev);
        if (next.has(node.id)) next.delete(node.id);
        else next.add(node.id);
        return next;
      });
      // Only fetch detail for real task nodes; workflow node has no DB row.
      if (workflowId && node.type === 'task') {
        fetchTaskDetail(workflowId, node.id).then(setSelected).catch(() => {});
      } else {
        setSelected(null);
      }
    },
    [workflowId],
  );

  const onPickHighlight = useCallback(
    (by: HighlightBy | null) => {
      setHighlightBy(by);
      if (!by || !workflowId) {
        setHighlightIds(new Set());
        return;
      }
      fetchHighlights(workflowId, by).then((ids) => {
        setHighlightIds(new Set(ids));
        if (tasks) {
          setCollapsed((prev) => {
            const next = new Set(prev);
            for (const id of ids) {
              for (const anc of ancestorsOf(workflowId, tasks, id)) next.delete(anc);
            }
            return next;
          });
        }
      });
    },
    [workflowId, tasks],
  );

  if (error) {
    return <div style={{ padding: 16, fontFamily: 'system-ui' }}>Error: {error}</div>;
  }
  if (!workflowId) {
    return (
      <div style={{ padding: 16, fontFamily: 'system-ui' }}>
        No current workflow. Run <code>poetry run python -m outbox.send_tasks</code>, wait for completion, then reload.
      </div>
    );
  }
  if (!tasks) {
    return <div style={{ padding: 16, fontFamily: 'system-ui' }}>Loading tasks…</div>;
  }

  return (
    <div style={{ position: 'fixed', inset: 0, paddingTop: 48 }}>
      <HighlightBar workflowId={workflowId} current={highlightBy} onPick={onPickHighlight} />
      {tasks.length === 0 ? (
        <div style={{ padding: 64, fontFamily: 'system-ui' }}>
          Workflow <code>{workflowId}</code> has no persisted tasks yet.
          <br />
          Wait for the workflow to complete (terminal states are persisted by <code>consume_results.py</code>), then reload.
        </div>
      ) : (
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          onNodeClick={onNodeClick}
          fitView
        >
          <Background />
          <Controls />
          <MiniMap pannable zoomable />
        </ReactFlow>
      )}
      <DetailPanel task={selected} onClose={() => setSelected(null)} />
    </div>
  );
}
