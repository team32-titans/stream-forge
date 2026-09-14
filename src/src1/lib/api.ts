/**
 * StreamForge Unified Frontend-to-Backend Communication Bridge
 * =============================================================
 * Handles bidirectional WebSocket connections with automated reconnect,
 * protocol detection (wss:// vs ws://), and seamless REST/FastAPI proxy fallback.
 */

export type ConnectionStatus =
  | 'CONNECTED_WS'
  | 'CONNECTED_HTTP'
  | 'CONNECTING'
  | 'DISCONNECTED'
  | 'FALLBACK_SIMULATION';

export interface WsServerMessage {
  type:
    | 'CONNECTION_ESTABLISHED'
    | 'TELEMETRY_BURST'
    | 'TELEMETRY_BATCH'
    | 'METRICS_UPDATE'
    | 'CHAOS_EVENT'
    | 'PYTHON_ENGINE_STATUS'
    | 'PONG'
    | 'ERROR';
  data?: any;
  events?: any[];
  metrics?: any;
  status?: string;
  workerId?: string;
  timestamp?: number;
  [key: string]: any;
}

export interface PythonEngineStatus {
  status: string;
  framework: string;
  python_version: string;
  port: number;
  uptime_seconds?: number;
  uptime_sec?: number;
  fleet_size: number;
  total_partitions: number;
  active_workers: number;
  healthy_workers: number;
  total_events: number;
  throughput: number;
  anomalies: number;
  partition_allocations?: Record<string, number[]>;
  recent_samples?: any[];
}

export interface StreamApiClientOptions {
  wsPath?: string;
  reconnectBaseMs?: number;
  maxReconnectAttempts?: number;
  heartbeatIntervalMs?: number;
}

export class StreamApiClient {
  private ws: WebSocket | null = null;
  private status: ConnectionStatus = 'DISCONNECTED';
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 8;
  private reconnectBaseMs = 1500;
  private reconnectTimer: number | null = null;
  private heartbeatTimer: number | null = null;
  private pollTimer: number | null = null;
  private wsPath: string = '/ws';
  private pingStartTime = 0;
  private currentLatencyMs = 12;

  // Listeners
  private statusListeners = new Set<(status: ConnectionStatus) => void>();
  private telemetryListeners = new Set<(events: any[]) => void>();
  private metricsListeners = new Set<(metrics: any) => void>();
  private chaosListeners = new Set<(chaosEvent: any) => void>();
  private pythonStatusListeners = new Set<(status: PythonEngineStatus) => void>();

  constructor(options?: StreamApiClientOptions) {
    if (options?.wsPath) this.wsPath = options.wsPath;
    if (options?.maxReconnectAttempts) this.maxReconnectAttempts = options.maxReconnectAttempts;
    if (options?.reconnectBaseMs) this.reconnectBaseMs = options.reconnectBaseMs;
  }

  public getStatus(): ConnectionStatus {
    return this.status;
  }

  public getLatency(): number {
    return this.currentLatencyMs;
  }

  public onStatusChange(cb: (status: ConnectionStatus) => void): () => void {
    this.statusListeners.add(cb);
    cb(this.status);
    return () => this.statusListeners.delete(cb);
  }

  public onTelemetry(cb: (events: any[]) => void): () => void {
    this.telemetryListeners.add(cb);
    return () => this.telemetryListeners.delete(cb);
  }

  public onMetrics(cb: (metrics: any) => void): () => void {
    this.metricsListeners.add(cb);
    return () => this.metricsListeners.delete(cb);
  }

  public onChaosEvent(cb: (chaosEvent: any) => void): () => void {
    this.chaosListeners.add(cb);
    return () => this.chaosListeners.delete(cb);
  }

  public onPythonStatus(cb: (status: PythonEngineStatus) => void): () => void {
    this.pythonStatusListeners.add(cb);
    return () => this.pythonStatusListeners.delete(cb);
  }

  private setStatus(newStatus: ConnectionStatus): void {
    if (this.status !== newStatus) {
      this.status = newStatus;
      this.statusListeners.forEach((cb) => cb(newStatus));
    }
  }

  /**
   * Builds the correct WebSocket URL matching the active deployment host and protocol.
   * Ensures wss:// is used when served over HTTPS to eliminate Mixed Content errors.
   */
  private getWebSocketUrl(): string {
    if (typeof window === 'undefined') return 'ws://localhost:3000/ws';
    const loc = window.location;
    const protocol = loc.protocol === 'https:' ? 'wss:' : 'ws:';
    return `${protocol}//${loc.host}${this.wsPath}`;
  }

  public connect(): void {
    if (typeof window === 'undefined') return;
    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }

    this.setStatus('CONNECTING');
    const url = this.getWebSocketUrl();

    try {
      this.ws = new WebSocket(url);

      this.ws.onopen = () => {
        this.reconnectAttempts = 0;
        this.setStatus('CONNECTED_WS');
        this.stopHttpFallbackPolling();
        this.startHeartbeat();

        // Send initial handshake
        this.send({
          action: 'CLIENT_HELLO',
          client: 'StreamForge-Web-Frontend',
          version: '2.4.0',
          timestamp: Date.now(),
        });
      };

      this.ws.onmessage = (event: MessageEvent) => {
        try {
          const msg: WsServerMessage = JSON.parse(event.data);
          this.handleServerMessage(msg);
        } catch (e) {
          console.warn('[StreamForge WebSocket] Non-JSON payload received:', event.data);
        }
      };

      this.ws.onerror = (err) => {
        // Suppress noisy error object; standard browser behavior triggers onclose next
        console.warn('[StreamForge WebSocket] Connection error, initiating recovery sequence.');
      };

      this.ws.onclose = (event) => {
        this.stopHeartbeat();
        this.ws = null;
        this.scheduleReconnect();
      };
    } catch (err) {
      console.warn('[StreamForge WebSocket] Failed to create WebSocket:', err);
      this.scheduleReconnect();
    }
  }

  private handleServerMessage(msg: WsServerMessage): void {
    switch (msg.type) {
      case 'CONNECTION_ESTABLISHED':
        this.setStatus('CONNECTED_WS');
        if (msg.config) {
          this.metricsListeners.forEach((cb) => cb(msg.config));
        }
        break;

      case 'PONG':
        if (this.pingStartTime > 0) {
          this.currentLatencyMs = Math.max(1, Date.now() - this.pingStartTime);
        }
        break;

      case 'TELEMETRY_BURST':
      case 'TELEMETRY_BATCH':
        if (msg.events && Array.isArray(msg.events)) {
          this.telemetryListeners.forEach((cb) => cb(msg.events!));
        }
        if (msg.metrics) {
          this.metricsListeners.forEach((cb) => cb(msg.metrics));
        }
        break;

      case 'METRICS_UPDATE':
        if (msg.metrics) {
          this.metricsListeners.forEach((cb) => cb(msg.metrics));
        }
        break;

      case 'CHAOS_EVENT':
        if (msg.data) {
          this.chaosListeners.forEach((cb) => cb(msg.data));
        }
        break;

      case 'PYTHON_ENGINE_STATUS':
        if (msg.data) {
          this.pythonStatusListeners.forEach((cb) => cb(msg.data));
        }
        break;

      default:
        break;
    }
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.heartbeatTimer = window.setInterval(() => {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.pingStartTime = Date.now();
        this.send({ action: 'PING', timestamp: this.pingStartTime });
      }
    }, 15000);
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer) return;

    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      // Fallback to seamless HTTP streaming / polling
      this.setStatus('CONNECTED_HTTP');
      this.startHttpFallbackPolling();
      return;
    }

    this.setStatus('DISCONNECTED');
    const delay = Math.min(10000, this.reconnectBaseMs * Math.pow(1.5, this.reconnectAttempts));
    this.reconnectAttempts++;

    this.reconnectTimer = window.setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  }

  /**
   * HTTP Fallback Polling
   * When WebSocket is unavailable, polls /api/stream/telemetry and Python engine status
   * so the entire application continues operating without user interruption.
   */
  private startHttpFallbackPolling(): void {
    if (this.pollTimer) return;
    this.pollTimer = window.setInterval(async () => {
      try {
        const [telemetryRes, pyRes] = await Promise.allSettled([
          this.fetchTelemetry(15),
          this.fetchPythonEngineStatus(),
        ]);

        if (telemetryRes.status === 'fulfilled' && telemetryRes.value?.events) {
          this.telemetryListeners.forEach((cb) => cb(telemetryRes.value.events));
        }

        if (pyRes.status === 'fulfilled' && pyRes.value) {
          this.pythonStatusListeners.forEach((cb) => cb(pyRes.value));
        }

        // Try reconnecting WebSocket in background every 30 seconds
        if (this.reconnectAttempts >= this.maxReconnectAttempts && Math.random() < 0.1) {
          this.reconnectAttempts = 0;
          this.connect();
        }
      } catch (e) {
        // Safe fallback
      }
    }, 2000);
  }

  private stopHttpFallbackPolling(): void {
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
  }

  public send(data: any): boolean {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      try {
        this.ws.send(typeof data === 'string' ? data : JSON.stringify(data));
        return true;
      } catch (e) {
        console.warn('[StreamForge WebSocket] Failed to send message:', e);
      }
    }
    return false;
  }

  // REST API Client Methods & FastAPI / Python Engine Proxies

  public async fetchStatus(): Promise<any> {
    const res = await fetch('/api/stream/status');
    if (!res.ok) throw new Error(`Status check failed: ${res.status}`);
    return res.json();
  }

  public async fetchTelemetry(limit = 30): Promise<any> {
    const res = await fetch(`/api/stream/telemetry?limit=${limit}`);
    if (!res.ok) throw new Error(`Telemetry fetch failed: ${res.status}`);
    return res.json();
  }

  public async fetchPythonEngineStatus(): Promise<PythonEngineStatus> {
    // Queries the Express reverse proxy which forwards directly to the Python engine on port 9102
    const res = await fetch('/api/py/status');
    if (!res.ok) throw new Error(`Python engine status failed: ${res.status}`);
    return res.json();
  }

  public async triggerChaos(workerId = 'worker-04'): Promise<any> {
    // First try sending over active WebSocket
    const sentOverWs = this.send({
      action: 'TRIGGER_CHAOS',
      workerId,
      timestamp: Date.now(),
    });

    // Also call REST endpoint to ensure sync with server state
    const res = await fetch('/api/chaos/trigger', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ workerId }),
    });
    if (!res.ok) throw new Error(`Chaos trigger failed: ${res.status}`);
    return res.json();
  }

  public async setRate(rate: number): Promise<void> {
    this.send({ action: 'SET_RATE', rate });
  }

  public async diagnoseColdChain(telemetry: any): Promise<any> {
    const res = await fetch('/api/model/diagnose', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(telemetry),
    });
    if (!res.ok) throw new Error(`Diagnosis failed: ${res.status}`);
    return res.json();
  }

  public async askArchitect(question: string, context?: any): Promise<any> {
    const res = await fetch('/api/model/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question, context }),
    });
    if (!res.ok) throw new Error(`Architect chat failed: ${res.status}`);
    return res.json();
  }

  public disconnect(): void {
    this.stopHeartbeat();
    this.stopHttpFallbackPolling();
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.setStatus('DISCONNECTED');
  }
}

// Global Singleton Instance
export const streamApi = new StreamApiClient();
