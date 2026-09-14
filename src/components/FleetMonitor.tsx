import React, { useState, useEffect } from 'react';
import {
  Activity,
  AlertTriangle,
  CheckCircle,
  Clock,
  Flame,
  Radio,
  Search,
  Sliders,
  Snowflake,
  Truck,
  Zap,
} from 'lucide-react';
import { streamSimulation } from '../engine/simulationEngine';
import { TelemetryEvent, WindowAggregate } from '../types/stream';

export const FleetMonitor: React.FC = () => {
  const [recentEvents, setRecentEvents] = useState<TelemetryEvent[]>(streamSimulation.recentEvents);
  const [aggregates, setAggregates] = useState<Map<string, WindowAggregate>>(
    new Map(streamSimulation.activeWindowAggregates)
  );
  const [searchTruck, setSearchTruck] = useState<string>('');
  const [selectedTruck, setSelectedTruck] = useState<string | null>(null);

  useEffect(() => {
    const unsubscribe = streamSimulation.subscribe(() => {
      setRecentEvents([...streamSimulation.recentEvents]);
      setAggregates(new Map(streamSimulation.activeWindowAggregates));
    });
    return unsubscribe;
  }, []);

  const filteredEvents = recentEvents.filter(
    (e) =>
      e.truckId.toLowerCase().includes(searchTruck.toLowerCase()) ||
      e.partition.toString() === searchTruck
  );

  const activeAgg = selectedTruck
    ? aggregates.get(selectedTruck)
    : aggregates.get(recentEvents[0]?.truckId);

  return (
    <div className="space-y-4">
      {/* Overview Header Bento Card */}
      <div className="bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-3xl p-6 shadow-xl relative overflow-hidden">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div className="flex items-center gap-3.5">
            <div className="w-10 h-10 rounded-xl bg-indigo-500/20 border border-indigo-500/40 flex items-center justify-center shadow-[0_0_15px_rgba(99,102,241,0.3)]">
              <Truck className="w-5 h-5 text-indigo-400" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h2 className="text-base font-bold text-white tracking-tight">
                  50,000 IOT FLEET TELEMETRY & COLD-CHAIN MONITOR
                </h2>
                <span className="text-[9px] px-2 py-0.5 rounded-full bg-slate-800 text-indigo-300 font-mono font-bold border border-slate-700 uppercase tracking-wider">
                  10s Cadence
                </span>
              </div>
              <p className="text-[11px] text-slate-400 uppercase tracking-wider mt-0.5">
                Real-time temperature streams partitioned across 32 Kafka topics, aggregated into 5-minute rolling averages
              </p>
            </div>
          </div>

          <div className="flex items-center gap-3 text-xs font-mono">
            <div className="bg-[#05070a] px-3.5 py-2 rounded-2xl border border-slate-700/60 text-slate-300 flex items-center gap-2">
              <Snowflake className="w-4 h-4 text-indigo-400" />
              <span>Target: <strong className="text-indigo-300">-20.0°C</strong></span>
            </div>
            <div className="bg-[#05070a] px-3.5 py-2 rounded-2xl border border-slate-700/60 text-slate-300 flex items-center gap-2">
              <Flame className="w-4 h-4 text-rose-400" />
              <span>Alarm: <strong className="text-rose-400">&gt; 0.0°C</strong></span>
            </div>
          </div>
        </div>
      </div>

      {/* Main Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Left 2 Cols: Live Telemetry Stream Table */}
        <div className="lg:col-span-2 bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-3xl p-6 shadow-xl space-y-4">
          <div className="flex items-center justify-between border-b border-slate-700/50 pb-3">
            <div className="flex items-center gap-2">
              <Radio className="w-4 h-4 text-indigo-400" />
              <h3 className="text-xs font-bold uppercase tracking-widest text-slate-200">
                Live Ingested Telemetry Feed (Zero-Copy Kafka Stream)
              </h3>
            </div>
            <input
              type="text"
              placeholder="Search truck or partition..."
              value={searchTruck}
              onChange={(e) => setSearchTruck(e.target.value)}
              className="bg-[#05070a] border border-slate-700/50 rounded-2xl px-3.5 py-1.5 text-xs text-slate-200 font-mono focus:outline-none focus:border-indigo-500 w-52"
            />
          </div>

          <div className="overflow-x-auto">
            <table className="w-full text-left font-mono text-xs">
              <thead>
                <tr className="border-b border-slate-700/60 text-slate-400 text-[10px] uppercase tracking-wider">
                  <th className="pb-2.5 font-bold">Truck ID</th>
                  <th className="pb-2.5 font-bold">Partition</th>
                  <th className="pb-2.5 font-bold">Cargo Temp</th>
                  <th className="pb-2.5 font-bold">5-Min Avg</th>
                  <th className="pb-2.5 font-bold">Speed</th>
                  <th className="pb-2.5 font-bold">Status</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60">
                {filteredEvents.slice(0, 10).map((evt, idx) => {
                  const agg = aggregates.get(evt.truckId);
                  const isCritical = evt.refrigerationStatus === 'CRITICAL' || evt.temperature > 0;

                  return (
                    <tr
                      key={idx}
                      onClick={() => setSelectedTruck(evt.truckId)}
                      className="hover:bg-slate-800/40 cursor-pointer transition text-slate-200"
                    >
                      <td className="py-2.5 text-indigo-300 font-bold">{evt.truckId}</td>
                      <td className="py-2.5 text-slate-400">P{evt.partition}</td>
                      <td className="py-2.5">
                        <span
                          className={`px-2 py-0.5 rounded-full font-bold ${
                            isCritical
                              ? 'bg-rose-500/20 text-rose-300 border border-rose-500/30'
                              : 'text-slate-100'
                          }`}
                        >
                          {evt.temperature}°C
                        </span>
                      </td>
                      <td className="py-2.5 text-emerald-400 font-bold">
                        {agg ? `${agg.avgTemp}°C` : 'Calculating...'}
                      </td>
                      <td className="py-2.5 text-slate-400">{evt.speedKmH} km/h</td>
                      <td className="py-2.5">
                        <span
                          className={`text-[9px] px-2 py-0.5 rounded-full font-bold uppercase ${
                            isCritical
                              ? 'bg-rose-500/20 text-rose-300 border border-rose-500/30'
                              : 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30'
                          }`}
                        >
                          {evt.refrigerationStatus}
                        </span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>

        {/* Right Col: Selected Truck Detail & 5-Min Rolling Window Card */}
        <div className="bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-3xl p-6 shadow-xl space-y-4 flex flex-col justify-between">
          <div>
            <div className="flex items-center justify-between border-b border-slate-700/50 pb-3 mb-4">
              <h3 className="text-xs font-bold uppercase tracking-widest text-slate-200 flex items-center gap-2">
                <Truck className="w-4 h-4 text-indigo-400" />
                Vehicle Detail: {activeAgg?.truckId || 'Select Truck'}
              </h3>
            </div>

            {activeAgg ? (
              <div className="space-y-3 text-xs">
                <div className="bg-[#05070a] p-4 rounded-2xl border border-slate-700/50 space-y-2">
                  <div className="text-[10px] text-slate-400 uppercase font-bold tracking-wider">
                    Active 5-Minute Window Bounds
                  </div>
                  <div className="flex justify-between font-mono text-[11px]">
                    <span className="text-slate-400 font-sans">Start:</span>
                    <span className="text-indigo-300">
                      {new Date(activeAgg.windowStart).toLocaleTimeString()}
                    </span>
                  </div>
                  <div className="flex justify-between font-mono text-[11px]">
                    <span className="text-slate-400 font-sans">End (Seal):</span>
                    <span className="text-indigo-300">
                      {new Date(activeAgg.windowEnd).toLocaleTimeString()}
                    </span>
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-2.5 font-mono">
                  <div className="bg-[#05070a] p-3 rounded-2xl border border-slate-700/50">
                    <div className="text-[9px] text-slate-400 uppercase font-sans">5-Min Rolling Avg</div>
                    <div className="text-base font-bold text-emerald-400 mt-1">
                      {activeAgg.avgTemp}°C
                    </div>
                  </div>

                  <div className="bg-[#05070a] p-3 rounded-2xl border border-slate-700/50">
                    <div className="text-[9px] text-slate-400 uppercase font-sans">Readings Count</div>
                    <div className="text-base font-bold text-indigo-300 mt-1">
                      {activeAgg.count} samples
                    </div>
                  </div>

                  <div className="bg-[#05070a] p-3 rounded-2xl border border-slate-700/50">
                    <div className="text-[9px] text-slate-400 uppercase font-sans">Min Observed</div>
                    <div className="text-sm font-bold text-slate-200 mt-1">
                      {activeAgg.minTemp}°C
                    </div>
                  </div>

                  <div className="bg-[#05070a] p-3 rounded-2xl border border-slate-700/50">
                    <div className="text-[9px] text-slate-400 uppercase font-sans">Max Observed</div>
                    <div className="text-sm font-bold text-slate-200 mt-1">
                      {activeAgg.maxTemp}°C
                    </div>
                  </div>
                </div>
              </div>
            ) : (
              <p className="text-xs text-slate-400">Click a truck in the table to inspect state.</p>
            )}
          </div>

          <div className="pt-3 border-t border-slate-700/50 text-[10px] text-slate-400 font-mono flex items-center justify-between">
            <span className="font-sans">State Store: RocksDB MemTable</span>
            <span className="text-emerald-400 font-bold">Synced to Changelog</span>
          </div>
        </div>
      </div>
    </div>
  );
};
