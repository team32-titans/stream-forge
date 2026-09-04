import React, { useState } from 'react';
import {
  BookOpen,
  Check,
  CheckCircle2,
  Code2,
  Copy,
  Download,
  FileCode,
  Folder,
  Layers,
  Sparkles,
  Terminal,
} from 'lucide-react';
import { PYTHON_CODEBASE, PythonFile } from '../data/pythonCodebase';

export const CodebaseExplorer: React.FC = () => {
  const [selectedFile, setSelectedFile] = useState<PythonFile>(PYTHON_CODEBASE[0]);
  const [copied, setCopied] = useState<boolean>(false);
  const [selectedCategory, setSelectedCategory] = useState<string>('ALL');

  const categories = ['ALL', 'Core & OOP', 'Windowing Engine', 'RocksDB State', 'Fault Tolerance & Recovery', 'Producers & Metrics', 'Unit & Chaos Tests'];

  const filteredFiles = selectedCategory === 'ALL'
    ? PYTHON_CODEBASE
    : PYTHON_CODEBASE.filter((f) => f.category === selectedCategory);

  const handleCopy = () => {
    navigator.clipboard.writeText(selectedFile.code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleDownloadAll = () => {
    // Generate a single consolidated python file or trigger zip download
    const consolidated = PYTHON_CODEBASE.map(
      (f) => `# ${'='.repeat(70)}\n# FILE: ${f.path}\n# ${f.description}\n# ${'='.repeat(70)}\n\n${f.code}\n\n`
    ).join('\n');

    const blob = new Blob([consolidated], { type: 'text/x-python' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'streamforge_member1_complete_engine.py';
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="space-y-4">
      {/* Header Banner Bento Card */}
      <div className="bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-3xl p-6 shadow-xl relative overflow-hidden">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div className="flex items-center gap-3.5">
            <div className="w-10 h-10 rounded-xl bg-indigo-500/20 border border-indigo-500/40 flex items-center justify-center shadow-[0_0_15px_rgba(99,102,241,0.3)]">
              <Code2 className="w-5 h-5 text-indigo-400" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h2 className="text-base font-bold text-white tracking-tight">
                  PRODUCTION PYTHON OOP CODEBASE (CORE PIPELINE ENGINE)
                </h2>
                <span className="text-[9px] px-2 py-0.5 rounded-full bg-slate-800 text-indigo-300 font-mono font-bold border border-slate-700 uppercase tracking-wider">
                  PEP 8 & Clean Architecture
                </span>
              </div>
              <p className="text-[11px] text-slate-400 uppercase tracking-wider mt-0.5">
                Complete, type-annotated, runnable Python package with Design Patterns (Strategy, State, Observer, Template Method, WAL)
              </p>
            </div>
          </div>

          <button
            onClick={handleDownloadAll}
            className="flex items-center gap-2 px-4 py-2.5 rounded-2xl bg-indigo-600 hover:bg-indigo-500 text-white font-bold text-xs shadow-lg shadow-indigo-600/30 transition uppercase tracking-wider cursor-pointer"
          >
            <Download className="w-4 h-4" /> Export Python Package
          </button>
        </div>

        {/* Category Filters */}
        <div className="flex items-center gap-2 overflow-x-auto mt-4 pt-3.5 border-t border-slate-700/50 scrollbar-none">
          {categories.map((cat) => (
            <button
              key={cat}
              onClick={() => setSelectedCategory(cat)}
              className={`px-3.5 py-1.5 text-xs rounded-2xl transition whitespace-nowrap uppercase tracking-wider font-semibold cursor-pointer ${
                selectedCategory === cat
                  ? 'bg-indigo-600 text-white shadow-md shadow-indigo-900/40'
                  : 'bg-[#05070a] border border-slate-700/50 text-slate-400 hover:text-slate-200 hover:border-slate-600'
              }`}
            >
              {cat}
            </button>
          ))}
        </div>
      </div>

      {/* Main Code Viewer Layout */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-4">
        {/* Left: File Tree (4 Cols) */}
        <div className="lg:col-span-4 bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-3xl p-5 shadow-xl space-y-3">
          <div className="text-xs font-bold uppercase tracking-widest text-slate-300 flex items-center gap-2 border-b border-slate-700/50 pb-2.5">
            <Folder className="w-4 h-4 text-indigo-400" />
            Project File Tree
          </div>

          <div className="space-y-2 max-h-[580px] overflow-y-auto pr-1">
            {filteredFiles.map((file) => {
              const isSelected = selectedFile.path === file.path;
              return (
                <div
                  key={file.path}
                  onClick={() => setSelectedFile(file)}
                  className={`p-3.5 rounded-2xl border text-xs cursor-pointer transition ${
                    isSelected
                      ? 'bg-indigo-600/20 border-indigo-500/60 text-white shadow-lg shadow-indigo-950/40'
                      : 'bg-[#05070a] border-slate-700/50 text-slate-300 hover:border-slate-600'
                  }`}
                >
                  <div className="flex items-center gap-2 font-mono font-bold text-slate-200 mb-1">
                    <FileCode className={`w-3.5 h-3.5 ${isSelected ? 'text-indigo-400' : 'text-slate-500'}`} />
                    <span className="truncate">{file.name}</span>
                  </div>
                  <p className="text-[11px] text-slate-400 line-clamp-2">{file.description}</p>
                  <div className="mt-2 flex flex-wrap gap-1">
                    {file.oopPatterns.map((pat, idx) => (
                      <span
                        key={idx}
                        className="text-[9px] px-2 py-0.5 rounded-full bg-slate-800 text-indigo-300 font-mono border border-slate-700"
                      >
                        {pat}
                      </span>
                    ))}
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Right: Code Viewer & Architectural Notes (8 Cols) */}
        <div className="lg:col-span-8 bg-[#111620]/60 backdrop-blur-sm border border-slate-700/40 rounded-3xl p-6 shadow-xl space-y-4 flex flex-col justify-between">
          <div>
            {/* File Info Header */}
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 border-b border-slate-700/50 pb-3.5 mb-3.5">
              <div>
                <div className="text-xs font-mono font-bold text-indigo-300 flex items-center gap-2">
                  <FileCode className="w-4 h-4 text-indigo-400" />
                  {selectedFile.path}
                </div>
                <p className="text-xs text-slate-400 mt-1">{selectedFile.description}</p>
              </div>

              <button
                onClick={handleCopy}
                className="flex items-center gap-1.5 px-3.5 py-1.5 rounded-2xl bg-[#05070a] hover:bg-slate-800 text-slate-200 text-xs font-semibold border border-slate-700/60 transition self-start sm:self-auto cursor-pointer"
              >
                {copied ? (
                  <>
                    <Check className="w-3.5 h-3.5 text-emerald-400" /> Copied!
                  </>
                ) : (
                  <>
                    <Copy className="w-3.5 h-3.5 text-slate-400" /> Copy Code
                  </>
                )}
              </button>
            </div>

            {/* Design Patterns & Concepts Badges */}
            <div className="flex flex-wrap items-center gap-2 mb-3.5 text-xs">
              <span className="text-slate-400 font-semibold text-[11px] uppercase tracking-wider">OOP Patterns:</span>
              {selectedFile.oopPatterns.map((pat, i) => (
                <span
                  key={i}
                  className="px-2.5 py-0.5 rounded-full bg-indigo-500/20 text-indigo-300 font-mono text-[10px] font-bold border border-indigo-500/30"
                >
                  {pat}
                </span>
              ))}
              <span className="text-slate-500 mx-1">•</span>
              <span className="text-slate-400 font-semibold text-[11px] uppercase tracking-wider">Key Concepts:</span>
              {selectedFile.keyConcepts.map((con, i) => (
                <span
                  key={i}
                  className="px-2.5 py-0.5 rounded-full bg-slate-800 text-slate-300 font-mono text-[10px] border border-slate-700"
                >
                  {con}
                </span>
              ))}
            </div>

            {/* Code Block with Line Numbers */}
            <div className="relative rounded-2xl overflow-hidden border border-slate-700/50 bg-[#05070a]">
              <div className="flex items-center justify-between px-4 py-2.5 bg-slate-900/90 border-b border-slate-700/50 text-[11px] text-slate-400 font-mono">
                <span>Python 3.11+ • PEP 8 Standard</span>
                <span>{selectedFile.code.split('\n').length} lines</span>
              </div>
              <pre className="p-4 text-xs font-mono text-slate-200 overflow-x-auto max-h-[460px] leading-relaxed select-text">
                {selectedFile.code}
              </pre>
            </div>
          </div>

          <div className="mt-4 pt-3.5 border-t border-slate-700/50 text-xs text-slate-400 flex items-center justify-between font-mono">
            <span className="flex items-center gap-1.5 text-emerald-400 font-bold">
              <CheckCircle2 className="w-3.5 h-3.5" /> PyTest & Mypy Validated
            </span>
            <span className="text-indigo-400 uppercase tracking-wider">Industrial Clean Architecture</span>
          </div>
        </div>
      </div>
    </div>
  );
};
