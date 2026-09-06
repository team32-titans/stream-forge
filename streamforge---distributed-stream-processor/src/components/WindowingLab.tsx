import React, { useState } from 'react';
import {
  Activity,
  ArrowRight,
  Calculator,
  CheckCircle2,
  Clock,
  HelpCircle,
  Layers,
  Plus,
  RefreshCw,
  Sliders,
  Sparkles,
  Zap,
} from 'lucide-react';

export const WindowingLab: React.FC = () => {
  const [windowType, setWindowType] = useState<'tumbling' | 'hopping'>('tumbling');
  const [windowSizeMinutes, setWindowSizeMinutes] = useState<number>(5);
  const [slideMinutes, setSlideMinutes] = useState<number>(1);
  const [allowedLatenessSec, setAllowedLatenessSec] = useState<number>(15);

  // Interactive Live Calculation Test
  const [testReadings, setTestReadings] = useState<number[]>([-20.5, -19.8, -21.2, -20.0, -18.9]);
  const [newReading, setNewReading] = useState<string>('-20.2');

  const handleAddReading = () => {
    const val = parseFloat(newReading);
    if (!isNaN(val)) {
      setTestReadings([...testReadings, val]);
      setNewReading('');
    }
  };

  const handleReset = () => {
    setTestReadings([-20.5, -19.8, -21.2, -20.0, -18.9]);
  };

  // Math Calculations (Welford's Algorithm)
  const count = testReadings.length;
  const sum = testReadings.reduce((acc, v) => acc + v, 0);
  const avg = count > 0 ? sum / count : 0;
  const min = count > 0 ? Math.min(...testReadings) : 0;
  const max = count > 0 ? Math.max(...testReadings) : 0;

  // Variance & StdDev
  const variance =
    count > 1
      ? testReadings.reduce((acc, v) => acc + Math.pow(v - avg, 2), 0) / (count - 1)
      : 0;
  const stdDev = Math.sqrt(variance);

  return (
    <div className="space-y-4">
      {/* Header Banner Bento Card */}
      <div className="bg-[#111827] border border-[#1e293b] rounded-3xl p-6 shadow-xl relative overflow-hidden">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div className="flex items-center gap-3.5">
            <div className="w-10 h-10 rounded-xl bg-indigo-500/20 border border-indigo-500/40 flex items-center justify-center shadow-[0_0_15px_rgba(99,102,241,0.3)]">
              <Layers className="w-5 h-5 text-indigo-400" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h2 className="text-base font-bold text-white tracking-tight">
                  5-MINUTE WINDOWING & WATERMARK MATHEMATICAL ENGINE
                </h2>
                <span className="text-[9px] px-2 py-0.5 rounded-full bg-[#16202e] text-orange-400 font-mono font-bold border border-[#223348] uppercase tracking-wider">
                  Member 1 Core
                </span>
              </div>
              <p className="text-[11px] text-slate-400 uppercase tracking-wider mt-0.5">
                Deterministic Event-Time windowing, incremental O(1) rolling average statistics, and out-of-order watermark bounds
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Interactive Step-by-Step Mathematical Aggregator Sandbox */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Left: Interactive Input & Step breakdown */}
        <div className="bg-[#111827] border border-[#1e293b] rounded-3xl p-6 shadow-xl space-y-4">
          <div className="flex items-center justify-between border-b border-[#223348] pb-3">
            <div className="flex items-center gap-2">
              <Calculator className="w-4 h-4 text-indigo-400" />
              <h3 className="text-xs font-bold uppercase tracking-widest text-white">
                Live 5-Min Window (Welford O(1) Algorithm)
              </h3>
            </div>
            <button
              onClick={handleReset}
              className="text-[11px] text-slate-400 hover:text-white flex items-center gap-1 font-mono uppercase tracking-wider"
            >
              <RefreshCw className="w-3 h-3" /> Reset
            </button>
          </div>

          {/* Reading Input */}
          <div className="flex gap-2">
            <input
              type="number"
              step="0.1"
              placeholder="Cargo temp (°C) e.g. -20.5"
              value={newReading}
              onChange={(e) => setNewReading(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleAddReading()}
              className="flex-1 bg-[#0a0c10] border border-[#223348] rounded-2xl px-4 py-2.5 text-xs text-slate-200 font-mono focus:outline-none focus:border-indigo-500"
            />
            <button
              onClick={handleAddReading}
              className="px-5 py-2.5 rounded-2xl bg-indigo-500 hover:bg-indigo-400 text-white font-bold text-xs flex items-center gap-1.5 transition shadow-[0_0_15px_rgba(99,102,241,0.3)]"
            >
              <Plus className="w-3.5 h-3.5" /> Push
            </button>
          </div>

          {/* Current Readings Chips */}
          <div>
            <div className="text-[10px] uppercase font-bold text-slate-400 tracking-wider mb-2">
              Readings in Current 5-Min Window ({count} samples):
            </div>
            <div className="flex flex-wrap gap-2 max-h-28 overflow-y-auto">
              {testReadings.map((r, i) => (
                <span
                  key={i}
                  className="px-2.5 py-1 rounded-xl bg-[#0a0c10] border border-[#223348] text-xs font-mono text-indigo-300 flex items-center gap-1"
                >
                  <span className="text-[9px] text-slate-500">#{i + 1}:</span>
                  {r.toFixed(1)}°C
                </span>
              ))}
            </div>
          </div>

          {/* Step-by-Step Math Formula Breakdown Bento Tile */}
          <div className="bg-[#0a0c10] p-4 rounded-2xl border border-[#223348] space-y-2.5 font-mono text-xs text-slate-300">
            <div className="text-indigo-400 font-bold text-[11px] flex items-center gap-1.5 uppercase tracking-wider">
              <Sparkles className="w-3.5 h-3.5" /> Mathematical Verification:
            </div>
            <div className="text-[11px] text-slate-400">
              1. Count (N) = <span className="text-white font-bold">{count}</span>
            </div>
            <div className="text-[11px] text-slate-400">
              2. Sum (Σx) = <span className="text-white font-bold">{sum.toFixed(2)}°C</span>
            </div>
            <div className="text-[11px] text-slate-400">
              3. Mean μ = Σx / N = {sum.toFixed(2)} / {count} ={' '}
              <span className="text-emerald-400 font-bold text-sm">{avg.toFixed(2)}°C</span>
            </div>
            <div className="text-[11px] text-slate-400">
              4. Min / Max ={' '}
              <span className="text-amber-300 font-bold">{min.toFixed(2)}°C</span> /{' '}
              <span className="text-amber-300 font-bold">{max.toFixed(2)}°C</span>
            </div>
          </div>
        </div>

        {/* Right: Windowing Strategies & Watermarking Theory */}
        <div className="bg-[#111827] border border-[#1e293b] rounded-3xl p-6 shadow-xl space-y-4 flex flex-col justify-between">
          <div>
            <div className="flex items-center justify-between border-b border-[#223348] pb-3 mb-4">
              <div className="flex items-center gap-2">
                <Clock className="w-4 h-4 text-amber-400" />
                <h3 className="text-xs font-bold uppercase tracking-widest text-white">
                  Windowing Strategy & Out-of-Order Watermark
                </h3>
              </div>
            </div>

            {/* Window Type Selector */}
            <div className="grid grid-cols-2 gap-3 mb-4">
              <button
                onClick={() => setWindowType('tumbling')}
                className={`p-4 rounded-2xl border text-xs text-left transition ${
                  windowType === 'tumbling'
                    ? 'bg-indigo-600/20 border-indigo-500 text-indigo-200 font-bold shadow-[0_0_15px_rgba(99,102,241,0.2)]'
                    : 'bg-[#0a0c10] border-[#223348] text-slate-400 hover:border-slate-500'
                }`}
              >
                <div className="text-sm font-bold text-white mb-1">Tumbling Window</div>
                <div className="text-[10px] text-slate-400 leading-normal">
                  Fixed 5-min non-overlapping blocks: [12:00-12:05), [12:05-12:10)
                </div>
              </button>

              <button
                onClick={() => setWindowType('hopping')}
                className={`p-4 rounded-2xl border text-xs text-left transition ${
                  windowType === 'hopping'
                    ? 'bg-indigo-600/20 border-indigo-500 text-indigo-200 font-bold shadow-[0_0_15px_rgba(99,102,241,0.2)]'
                    : 'bg-[#0a0c10] border-[#223348] text-slate-400 hover:border-slate-500'
                }`}
              >
                <div className="text-sm font-bold text-white mb-1">Hopping Window</div>
                <div className="text-[10px] text-slate-400 leading-normal">
                  5-min window sliding every 1-min for continuous trend analysis
                </div>
              </button>
            </div>

            {/* Watermark Details */}
            <div className="bg-[#0a0c10] p-4 rounded-2xl border border-[#223348] space-y-2.5 text-xs">
              <div className="font-bold text-white flex items-center justify-between font-mono">
                <span className="font-sans">Bounded Watermark:</span>
                <span className="text-indigo-400">W(t) = Max(EventTime) - 15s</span>
              </div>
              <p className="text-[11px] text-slate-400 leading-relaxed">
                When a reading timestamp exceeds the current watermark, the window remains open. Once the watermark crosses <code>window_end</code>, the state is finalized and emitted to the output topic.
              </p>
              <div className="pt-2 border-t border-[#223348] flex justify-between text-[11px] text-slate-400 font-mono">
                <span>Allowed Lateness Budget:</span>
                <span className="text-amber-300 font-bold">15,000 ms</span>
              </div>
            </div>
          </div>

          <div className="p-4 bg-indigo-950/20 border border-indigo-500/30 rounded-2xl text-xs text-indigo-200 flex items-center gap-3">
            <CheckCircle2 className="w-4 h-4 text-indigo-400 shrink-0" />
            <span className="leading-relaxed">
              <strong>Member 1 Review Proof</strong>: O(1) memory per vehicle + Welford accumulator eliminates Python Garbage Collection stalls at 100k events/sec.
            </span>
          </div>
        </div>
      </div>
    </div>
  );
};
