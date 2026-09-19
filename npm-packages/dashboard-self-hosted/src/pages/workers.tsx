import { useEffect, useState } from "react";
import { Sheet } from "@ui/Sheet";
import { useAdminKey, useDeploymentUrl } from "@common/lib/deploymentApi";

const POLL_INTERVAL_MS = 2000;

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

function WorkerPoolTable({
  title,
  workers,
}: {
  title: string;
  workers: WorkerStatus[];
}) {
  return (
    <Sheet>
      <h2 className="mb-2 font-semibold">{title}</h2>
      <table className="w-full text-left text-sm">
        <thead>
          <tr className="border-b">
            <th className="py-1 pr-4">Address</th>
            <th className="py-1 pr-4">Healthy</th>
            <th className="py-1 pr-4">Load</th>
            <th className="py-1 pr-4">In flight</th>
            <th className="py-1">Last report</th>
          </tr>
        </thead>
        <tbody>
          {workers.length === 0 ? (
            <tr>
              <td className="py-1 text-content-secondary" colSpan={5}>
                No workers
              </td>
            </tr>
          ) : (
            workers.map((w) => (
              <tr key={w.addr} className="border-b last:border-0">
                <td className="py-1 pr-4">{w.addr}</td>
                <td className="py-1 pr-4">{w.healthy ? "up" : "down"}</td>
                <td className="py-1 pr-4">{w.load.toFixed(2)}</td>
                <td className="py-1 pr-4">{w.inFlight}</td>
                <td className="py-1">
                  {w.lastReportMs === null
                    ? "never"
                    : `${(w.lastReportMs / 1000).toFixed(1)}s`}
                </td>
              </tr>
            ))
          )}
        </tbody>
      </table>
    </Sheet>
  );
}

function Workers() {
  const { status, error } = useFunrunStatus();

  if (error) {
    return <div className="p-6 text-content-error">{error}</div>;
  }
  if (!status) {
    return <div className="p-6 text-content-secondary">Loading…</div>;
  }

  const { conductor, pools } = status;
  return (
    <div className="flex flex-col gap-4 p-6">
      <Sheet>
        <p>
          Conductor {conductor.version} · up {conductor.uptimeS}s · fallback{" "}
          {conductor.fallback} · fallbacks isolate=
          {conductor.fallbackTotal.isolate} deploy=
          {conductor.fallbackTotal.deploy} node={conductor.fallbackTotal.node}
        </p>
      </Sheet>
      <WorkerPoolTable title="Isolate pool" workers={pools.isolate} />
      {pools.node === null ? (
        <Sheet>
          <h2 className="mb-2 font-semibold">Node pool</h2>
          <p className="text-content-secondary">
            Node actions run on the conductor (FUNRUN_NODE_WORKERS unset).
          </p>
        </Sheet>
      ) : (
        <WorkerPoolTable title="Node pool" workers={pools.node} />
      )}
    </div>
  );
}

export default Workers;
