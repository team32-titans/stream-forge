import { useEffect, useState } from "react";
import { fetchMetrics, metricsWsUrl, IS_DEMO } from "../lib/api";

export function useLiveMetrics(pollMs = 1000) {
  const [data, setData] = useState<{ counters: Record<string, number>; gauges: Record<string, number> } | null>(null);
  const [live, setLive] = useState(false);

  useEffect(() => {
    if (IS_DEMO) return;
    let ws: WebSocket | null = null;
    let poll: number | null = null;
    let closed = false;

    const tryWs = () => {
      try {
        ws = new WebSocket(metricsWsUrl());
        ws.onopen = () => setLive(true);
        ws.onmessage = (ev) => {
          try {
            const j = JSON.parse(ev.data);
            if (j.counters) setData({ counters: j.counters, gauges: j.gauges });
          } catch {}
        };
        ws.onclose = () => {
          if (!closed) {
            setLive(false);
            // fallback poll
            poll = window.setInterval(async () => {
              try {
                const m = await fetchMetrics();
                setData(m);
                setLive(true);
              } catch {
                setLive(false);
              }
            }, pollMs);
          }
        };
        ws.onerror = () => {
          try { ws?.close(); } catch {}
        };
      } catch {
        // poll fallback
        poll = window.setInterval(async () => {
          try { const m = await fetchMetrics(); setData(m); setLive(true); } catch { setLive(false); }
        }, pollMs);
      }
    };
    tryWs();

    // also initial fetch
    fetchMetrics().then(setData).catch(() => {});

    return () => {
      closed = true;
      try { ws?.close(); } catch {}
      if (poll) clearInterval(poll);
    };
  }, [pollMs]);

  return { data, live, isDemo: IS_DEMO };
}
