import {
  type CSSProperties,
  type ReactNode,
  useEffect,
  useMemo,
  useState,
} from "react";
// Imported here rather than via globals.css: dashboard-common hit a bug
// where a relative @import was dropped in production builds.
import "@xyflow/react/dist/style.css";
import {
  Background,
  Controls,
  type Edge,
  Handle,
  type Node,
  type NodeProps,
  Position,
  ReactFlow,
  ReactFlowProvider,
  useNodesState,
} from "@xyflow/react";
import {
  CheckCircledIcon,
  CrossCircledIcon,
  ExclamationTriangleIcon,
} from "@radix-ui/react-icons";
import { cn } from "@ui/cn";
import { Sheet } from "@ui/Sheet";
import { Tooltip } from "@ui/Tooltip";
import { HelpTooltip } from "@ui/HelpTooltip";
import { Callout } from "@ui/Callout";
import { Loading } from "@ui/Loading";
import { ProgressBarWithPercent } from "@ui/ProgressBar";
import { HealthCard } from "@common/elements/HealthCard";
import { BigMetric, MetricHealth } from "@common/elements/BigMetric";
import { PageContent } from "@common/elements/PageContent";
import { DeploymentPageTitle } from "@common/elements/DeploymentPageTitle";
import { TimestampDistance } from "@common/elements/TimestampDistance";
import { useAdminKey, useDeploymentUrl } from "@common/lib/deploymentApi";

const POLL_INTERVAL_MS = 2000;
// Workers report load roughly once a second; a report older than this means
// the conductor's view of that worker is out of date.
const STALE_REPORT_MS = 2000;

type WorkerStatus = {
  addr: string;
  healthy: boolean;
  load: number;
  inFlight: number;
  lastReportMs: number | null;
};

type FunrunStatus = {
  conductor: {
    version: string;
    uptimeS: number;
    fallback: string;
    fallbackTotal: { isolate: number; deploy: number; node: number };
  };
  pools: {
    isolate: WorkerStatus[];
    node: WorkerStatus[] | null;
  };
};

function useFunrunStatus() {
  const deploymentUrl = useDeploymentUrl();
  const adminKey = useAdminKey();
  const [status, setStatus] = useState<FunrunStatus | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function poll() {
      try {
        const res = await fetch(`${deploymentUrl}/api/funrun/status`, {
          headers: { Authorization: `Convex ${adminKey}` },
        });
        if (!res.ok) {
          if (!cancelled) setError(`${res.status} ${res.statusText}`);
          return;
        }
        const data = (await res.json()) as FunrunStatus;
        if (!cancelled) {
          setStatus(data);
          setError(null);
        }
      } catch (e) {
        if (!cancelled) setError(String(e));
      }
    }
    void poll();
    const interval = setInterval(() => void poll(), POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [deploymentUrl, adminKey]);

  return { status, error };
}

function isStale(lastReportMs: number | null): boolean {
  return lastReportMs !== null && lastReportMs > STALE_REPORT_MS;
}

function poolHealth(workers: WorkerStatus[]): MetricHealth {
  if (workers.some((w) => !w.healthy)) return "error";
  if (workers.some((w) => isStale(w.lastReportMs))) return "warning";
  return "healthy";
}

function averageLoad(workers: WorkerStatus[]): number {
  if (workers.length === 0) return 0;
  return workers.reduce((sum, w) => sum + w.load, 0) / workers.length;
}

function formatReportAge(lastReportMs: number | null): string {
  if (lastReportMs === null) return "n/a";
  if (lastReportMs < 1000) return `${Math.round(lastReportMs)}ms`;
  return `${(lastReportMs / 1000).toFixed(1)}s`;
}

function HealthIcon({
  health,
  className,
}: {
  health: MetricHealth;
  className?: string;
}) {
  if (health === "error") {
    return <CrossCircledIcon className={cn("text-content-error", className)} />;
  }
  if (health === "warning") {
    return (
      <ExclamationTriangleIcon
        className={cn("text-content-warning", className)}
      />
    );
  }
  return <CheckCircledIcon className={cn("text-content-success", className)} />;
}

// --- Topology canvas -------------------------------------------------------
// Same canvas idiom as the schema page — cards on a dotted grid that you can
// pan, zoom and drag. React Flow already ships with the dashboard for the
// schema viewer, so the topology reuses it rather than hand-rolling a diagram.
const CARD_W = 248;
const CARD_H = 104;
const CONDUCTOR_W = 216;
const CONDUCTOR_H = 132;
const HEADER_H = 30;
const CARD_GAP = 28;
// The conductor sits at the origin; workers fan out into a column to its right.
const COLUMN_X = 360;

type PoolKind = "isolate" | "node";

type ConductorData = { conductor: FunrunStatus["conductor"] };
type WorkerData = { pool: PoolKind; worker: WorkerStatus };
// A pool with nothing to draw still gets a card, so the graph never has a
// dangling edge: either the pool is off, or it is on and empty (a problem).
type NoteData = {
  pool: PoolKind;
  title: string;
  body: string;
  health: MetricHealth;
};

type TopologyNode =
  | Node<ConductorData, "conductor">
  | Node<WorkerData, "worker">
  | Node<NoteData, "note">;

const CONDUCTOR_ID = "conductor";

function poolNodes(
  pool: PoolKind,
  workers: WorkerStatus[] | null,
  emptyBody: string,
  disabledBody: string,
): TopologyNode[] {
  const at = { x: COLUMN_X, y: 0 };
  if (workers === null) {
    return [
      {
        id: `${pool}-disabled`,
        type: "note",
        position: at,
        data: {
          pool,
          title: "Not configured",
          body: disabledBody,
          health: "healthy",
        },
      },
    ];
  }
  if (workers.length === 0) {
    return [
      {
        id: `${pool}-empty`,
        type: "note",
        position: at,
        data: {
          pool,
          title: "No workers registered",
          body: emptyBody,
          health: "error",
        },
      },
    ];
  }
  return workers.map((worker) => ({
    id: `${pool}-${worker.addr}`,
    type: "worker" as const,
    position: at,
    data: { pool, worker },
  }));
}

function nodeHealth(node: TopologyNode): MetricHealth {
  if (node.type === "note") return node.data.health;
  if (node.type !== "worker") return "healthy";
  if (!node.data.worker.healthy) return "error";
  return isStale(node.data.worker.lastReportMs) ? "warning" : "healthy";
}

function buildTopology(status: FunrunStatus): {
  nodes: TopologyNode[];
  edges: Edge[];
} {
  const spokes = [
    ...poolNodes(
      "isolate",
      status.pools.isolate,
      "The conductor has no isolate worker to route to.",
      "",
    ),
    ...poolNodes(
      "node",
      status.pools.node,
      "The node pool is enabled but nothing has registered.",
      "Node actions run on the conductor. Set FUNRUN_NODE_WORKERS to enable a dedicated pool.",
    ),
  ];

  const columnH = spokes.length * CARD_H + (spokes.length - 1) * CARD_GAP;
  const positioned = spokes.map((node, i) => ({
    ...node,
    position: { x: COLUMN_X, y: i * (CARD_H + CARD_GAP) },
  }));

  const conductor: TopologyNode = {
    id: CONDUCTOR_ID,
    type: "conductor",
    position: { x: 0, y: (columnH - CONDUCTOR_H) / 2 },
    data: { conductor: status.conductor },
  };

  const edges: Edge[] = positioned.map((node) => {
    const health = nodeHealth(node);
    return {
      id: `${CONDUCTOR_ID}->${node.id}`,
      source: CONDUCTOR_ID,
      target: node.id,
      animated: health === "healthy" && node.type === "worker",
      style: {
        strokeWidth: 1.5,
        ...(health === "error" && {
          stroke: "var(--content-error)",
          strokeDasharray: "4 4",
        }),
        ...(health === "warning" && { stroke: "var(--content-warning)" }),
      },
    };
  });

  return { nodes: [conductor, ...positioned], edges };
}

// Handles exist so React Flow has something to anchor each edge to; the
// topology is not editable, so they stay invisible.
function EdgeAnchor({ type }: { type: "source" | "target" }) {
  return (
    <Handle
      type={type}
      position={type === "source" ? Position.Right : Position.Left}
      isConnectable={false}
      style={{
        width: 1,
        height: 1,
        minWidth: 1,
        minHeight: 1,
        border: 0,
        background: "transparent",
      }}
    />
  );
}

function PoolBadge({ pool }: { pool: PoolKind }) {
  return (
    <span className="ml-auto shrink-0 rounded-sm border px-1 text-[10px] font-medium text-content-tertiary">
      {pool}
    </span>
  );
}

function NodeRow({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="flex items-baseline justify-between gap-2 px-2.5 text-xs">
      <span className="text-content-tertiary">{label}</span>
      {children}
    </div>
  );
}

function NodeValue({
  value,
  className,
}: {
  value: string;
  className?: string;
}) {
  return (
    <span
      className={cn(
        "truncate font-mono text-content-secondary tabular-nums",
        className,
      )}
    >
      {value}
    </span>
  );
}

function NodeCard({
  health,
  dashed,
  width,
  height,
  children,
}: {
  health: MetricHealth;
  dashed?: boolean;
  width: number;
  height: number;
  children: ReactNode;
}) {
  return (
    <div
      className={cn(
        "flex flex-col overflow-hidden rounded-lg border bg-background-secondary",
        dashed && "border-dashed",
        health === "error" && "border-content-error/50",
        health === "warning" && "border-content-warning/50",
      )}
      style={{ width, height }}
    >
      {children}
    </div>
  );
}

function WorkerFlowNode({ data }: NodeProps<Node<WorkerData, "worker">>) {
  const { worker, pool } = data;
  const stale = isStale(worker.lastReportMs);
  const health: MetricHealth = !worker.healthy
    ? "error"
    : stale
      ? "warning"
      : "healthy";
  return (
    <NodeCard health={health} width={CARD_W} height={CARD_H}>
      <EdgeAnchor type="target" />
      <div
        className="flex items-center gap-1.5 border-b bg-background-tertiary px-2.5"
        style={{ height: HEADER_H }}
      >
        <HealthIcon health={health} className="size-3.5 shrink-0" />
        <span
          className="truncate font-mono text-xs text-content-primary"
          title={worker.addr}
        >
          {worker.addr}
        </span>
        <PoolBadge pool={pool} />
      </div>
      <div className="flex flex-1 flex-col justify-center gap-1">
        <NodeRow label="load">
          <NodeValue value={`${(worker.load * 100).toFixed(0)}%`} />
        </NodeRow>
        <NodeRow label="in flight">
          <NodeValue value={String(worker.inFlight)} />
        </NodeRow>
        <NodeRow label="last report">
          <NodeValue
            value={formatReportAge(worker.lastReportMs)}
            className={stale ? "text-content-warning" : undefined}
          />
        </NodeRow>
      </div>
      <div className="h-1 w-full bg-background-tertiary" aria-hidden>
        <div
          className="h-full bg-util-accent"
          style={{
            width: `${Math.min(100, Math.max(0, worker.load * 100))}%`,
          }}
        />
      </div>
    </NodeCard>
  );
}

function NoteFlowNode({ data }: NodeProps<Node<NoteData, "note">>) {
  return (
    <NodeCard health={data.health} dashed width={CARD_W} height={CARD_H}>
      <EdgeAnchor type="target" />
      <div className="flex flex-1 flex-col justify-center gap-1.5 px-3">
        <div className="flex items-center gap-1.5">
          {data.health !== "healthy" && (
            <HealthIcon health={data.health} className="size-3.5 shrink-0" />
          )}
          <span className="truncate text-xs font-medium text-content-primary">
            {data.title}
          </span>
          <PoolBadge pool={data.pool} />
        </div>
        <span className="text-xs leading-snug text-content-secondary">
          {data.body}
        </span>
      </div>
    </NodeCard>
  );
}

function ConductorFlowNode({
  data,
}: NodeProps<Node<ConductorData, "conductor">>) {
  const { conductor } = data;
  const startedAt = new Date(Date.now() - conductor.uptimeS * 1000);
  return (
    <NodeCard health="healthy" width={CONDUCTOR_W} height={CONDUCTOR_H}>
      <div
        className="flex items-center gap-1.5 border-b bg-background-tertiary px-2.5"
        style={{ height: HEADER_H }}
      >
        <span className="text-xs font-semibold tracking-wide text-content-primary uppercase">
          Conductor
        </span>
      </div>
      <div className="flex flex-1 flex-col justify-center gap-1">
        <NodeRow label="version">
          <NodeValue
            value={conductor.version === "unknown" ? "—" : conductor.version}
          />
        </NodeRow>
        <NodeRow label="fallback">
          <NodeValue value={conductor.fallback} />
        </NodeRow>
        <NodeRow label="started">
          <TimestampDistance date={startedAt} />
        </NodeRow>
      </div>
      <EdgeAnchor type="source" />
    </NodeCard>
  );
}

const topologyNodeTypes = {
  conductor: ConductorFlowNode,
  worker: WorkerFlowNode,
  note: NoteFlowNode,
};

// React Flow themes its controls through CSS variables rather than classes,
// which is the only hook that reaches inside its own button markup.
const CONTROLS_THEME = {
  "--xy-controls-button-background-color": "var(--background-secondary)",
  "--xy-controls-button-background-color-hover": "var(--background-tertiary)",
  "--xy-controls-button-color": "var(--content-primary)",
  "--xy-controls-button-color-hover": "var(--content-primary)",
  "--xy-controls-button-border-color": "var(--border-transparent)",
} as CSSProperties;

const FIT_VIEW_OPTIONS = { padding: 0.2, minZoom: 0.2, maxZoom: 1 };

function TopologyFlow({ status }: { status: FunrunStatus }) {
  const { nodes: built, edges } = useMemo(
    () => buildTopology(status),
    [status],
  );
  const [nodes, setNodes, onNodesChange] = useNodesState<TopologyNode>(built);

  // Status refreshes every couple of seconds; fold the new data into the nodes
  // already on the canvas so a worker the operator dragged stays put.
  useEffect(() => {
    setNodes((current) => {
      const placed = new Map(current.map((node) => [node.id, node.position]));
      return built.map((node) => ({
        ...node,
        position: placed.get(node.id) ?? node.position,
      }));
    });
  }, [built, setNodes]);

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      nodeTypes={topologyNodeTypes}
      onNodesChange={onNodesChange}
      nodesConnectable={false}
      deleteKeyCode={null}
      minZoom={0.2}
      maxZoom={1.5}
      fitView
      fitViewOptions={FIT_VIEW_OPTIONS}
      proOptions={{ hideAttribution: true }}
      className="bg-background-primary"
      role="application"
      aria-label="Worker topology. The conductor routes requests to the workers it is connected to. Drag to pan, scroll to zoom."
    >
      <Background gap={24} size={1} className="text-border-transparent" />
      <Controls showInteractive={false} style={CONTROLS_THEME} />
    </ReactFlow>
  );
}

// The operator's mental model at a glance: the conductor dispatches to the
// isolate pool, and — when configured — to a separate node pool.
function Topology({ status }: { status: FunrunStatus }) {
  return (
    <Sheet padding={false}>
      <div className="flex items-center gap-1.5 border-b p-4">
        <h5 className="text-content-primary">Topology</h5>
        <HelpTooltip>
          The conductor routes each request to a healthy worker in the matching
          pool. When a pool has no healthy worker, the request falls back to
          running in the backend process (or fails, depending on the fallback
          policy). Drag to pan, scroll to zoom, drag a card to move it.
        </HelpTooltip>
      </div>
      <div className="h-96 w-full">
        <ReactFlowProvider>
          <TopologyFlow status={status} />
        </ReactFlowProvider>
      </div>
    </Sheet>
  );
}

function PoolSummaryCard({
  title,
  tip,
  workers,
  disabledMessage = "Not configured.",
}: {
  title: string;
  tip: string;
  workers: WorkerStatus[] | null;
  disabledMessage?: string;
}) {
  if (workers === null) {
    return (
      <HealthCard title={title} tip={tip}>
        <div className="flex flex-col items-center gap-1 px-4 py-6 text-center">
          <span className="text-sm text-content-primary">Not configured</span>
          <span className="max-w-56 text-xs text-content-secondary">
            {disabledMessage}
          </span>
        </div>
      </HealthCard>
    );
  }

  if (workers.length === 0) {
    return (
      <HealthCard
        title={title}
        tip={tip}
        error="No workers registered in this pool."
      >
        <BigMetric health="error" metric="0">
          workers online
        </BigMetric>
      </HealthCard>
    );
  }

  const health = poolHealth(workers);
  const healthy = workers.filter((w) => w.healthy).length;
  return (
    <HealthCard
      title={title}
      tip={tip}
      warning={
        health === "warning" ? "A worker's load report is stale." : undefined
      }
      error={
        health === "error"
          ? "No healthy workers — requests may fail or fall back."
          : undefined
      }
    >
      <BigMetric health={health} metric={`${healthy}/${workers.length}`}>
        healthy workers
      </BigMetric>
      <div className="w-full px-4 pb-4">
        <ProgressBarWithPercent
          fraction={averageLoad(workers)}
          variant="solid"
          ariaLabel={`${title} average load`}
        />
      </div>
    </HealthCard>
  );
}

function ConductorSummaryCard({
  conductor,
}: {
  conductor: FunrunStatus["conductor"];
}) {
  const { fallbackTotal } = conductor;
  const total =
    fallbackTotal.isolate + fallbackTotal.deploy + fallbackTotal.node;
  const health: MetricHealth = total > 0 ? "warning" : "healthy";
  return (
    <HealthCard
      title="Fallbacks"
      tip="Requests that ran in the backend process because their pool had no healthy worker. Zero is the happy path."
      warning={
        total > 0 ? "Some requests fell back to local execution." : undefined
      }
    >
      <BigMetric health={health} metric={String(total)}>
        requests fell back to local execution
      </BigMetric>
      <div className="flex w-full justify-center gap-4 px-4 pb-4 text-xs text-content-secondary">
        <span>isolate {fallbackTotal.isolate}</span>
        <span>deploy {fallbackTotal.deploy}</span>
        <span>node {fallbackTotal.node}</span>
      </div>
    </HealthCard>
  );
}

function WorkerPoolTable({
  title,
  workers,
}: {
  title: string;
  workers: WorkerStatus[];
}) {
  return (
    <Sheet padding={false}>
      <div className="flex items-center justify-between border-b p-4">
        <h5 className="text-content-primary">{title}</h5>
        <span className="text-xs text-content-secondary">
          {workers.length} worker{workers.length === 1 ? "" : "s"}
        </span>
      </div>
      {workers.length === 0 ? (
        <p className="p-4 text-sm text-content-secondary">
          No workers reporting.
        </p>
      ) : (
        <table className="w-full text-left text-sm">
          <thead>
            <tr className="border-b text-xs text-content-secondary">
              <th className="px-4 py-2 font-medium">Status</th>
              <th className="px-4 py-2 font-medium">Address</th>
              <th className="w-48 px-4 py-2 font-medium">Load</th>
              <th className="px-4 py-2 font-medium">In flight</th>
              <th className="px-4 py-2 font-medium">Last report</th>
            </tr>
          </thead>
          <tbody>
            {workers.map((w) => {
              const stale = isStale(w.lastReportMs);
              return (
                <tr key={w.addr} className="border-b last:border-0">
                  <td className="px-4 py-2">
                    <Tooltip
                      tip={
                        w.healthy
                          ? "Healthy"
                          : "Unhealthy — the conductor will not route to this worker"
                      }
                    >
                      {w.healthy ? (
                        <CheckCircledIcon className="text-content-success" />
                      ) : (
                        <CrossCircledIcon className="text-content-error" />
                      )}
                    </Tooltip>
                  </td>
                  <td className="px-4 py-2 font-mono text-xs text-content-primary">
                    {w.addr}
                  </td>
                  <td className="px-4 py-2">
                    <ProgressBarWithPercent
                      fraction={w.load}
                      variant="solid"
                      ariaLabel={`Load for ${w.addr}`}
                    />
                  </td>
                  <td className="px-4 py-2 tabular-nums">{w.inFlight}</td>
                  <td className="px-4 py-2">
                    <span
                      className={cn(
                        "tabular-nums",
                        stale && "text-content-warning",
                      )}
                    >
                      {formatReportAge(w.lastReportMs)}
                    </span>
                    {stale && (
                      <Tooltip tip="No load report in over 2s — this worker's status may be out of date.">
                        <ExclamationTriangleIcon className="ml-1 inline size-3.5 text-content-warning" />
                      </Tooltip>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </Sheet>
  );
}

function Workers() {
  const { status, error } = useFunrunStatus();

  return (
    <PageContent>
      <DeploymentPageTitle title="Workers" />
      <div className="flex h-full flex-col gap-4 overflow-y-auto p-6">
        {error && <Callout variant="error">{error}</Callout>}
        {!error && !status && <Loading className="h-40" />}
        {!error && status && (
          <>
            <Topology status={status} />
            <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
              <PoolSummaryCard
                title="Isolate pool"
                tip="Runs JavaScript UDFs — queries, mutations, actions, and deploy-time evaluation."
                workers={status.pools.isolate}
              />
              <PoolSummaryCard
                title="Node pool"
                tip='Runs "use node" actions in real Node.js processes. Optional: when disabled, node actions run on the conductor instead.'
                workers={status.pools.node}
                disabledMessage="Node actions run on the conductor. Set FUNRUN_NODE_WORKERS to enable a dedicated pool."
              />
              <ConductorSummaryCard conductor={status.conductor} />
            </div>
            <WorkerPoolTable
              title="Isolate pool workers"
              workers={status.pools.isolate}
            />
            {status.pools.node !== null && (
              <WorkerPoolTable
                title="Node pool workers"
                workers={status.pools.node}
              />
            )}
          </>
        )}
      </div>
    </PageContent>
  );
}

export default Workers;
