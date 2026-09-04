// Live API client — used when VITE_DEMO_MODE !== "true"
const API_BASE = import.meta.env.VITE_API_URL || "http://localhost:8000";

export type ApiMetrics = { counters: Record<string, number>; gauges: Record<string, number> };
export type PartitionRes = { topic: string; partitions: { partitionId: number; leader: number }[] };

export async function fetchHealth() {
  const r = await fetch(`${API_BASE}/api/health`);
  if (!r.ok) throw new Error(`health ${r.status}`);
  return r.json();
}
export async function fetchMetrics(): Promise<ApiMetrics> {
  const r = await fetch(`${API_BASE}/api/metrics`);
  if (!r.ok) throw new Error(`metrics ${r.status}`);
  return r.json();
}
export async function fetchPartitions(): Promise<PartitionRes> {
  const r = await fetch(`${API_BASE}/api/partitions`);
  if (!r.ok) throw new Error(`partitions ${r.status}`);
  return r.json();
}
export async function fetchWorkers() {
  const r = await fetch(`${API_BASE}/api/workers`);
  if (!r.ok) throw new Error(`workers ${r.status}`);
  return r.json();
}
export function metricsWsUrl() {
  const base = API_BASE.replace(/^http/, "ws");
  return `${base}/ws/metrics`;
}
export const IS_DEMO = (import.meta.env.VITE_DEMO_MODE === "true") || new URLSearchParams(window.location.search).has("demo");
