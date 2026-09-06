# StreamForge (SteamForge)

> **Enterprise-Grade Distributed Stateful Event Streaming Engine & Real-Time Observability Control Plane**

[![React](https://img.shields.io/badge/React-18.x-61DAFB?style=for-the-badge&logo=react&logoColor=black)](https://reactjs.org/)
[![Vite](https://img.shields.io/badge/Vite-6.x-646CFF?style=for-the-badge&logo=vite&logoColor=white)](https://vitejs.dev/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.x-3178C6?style=for-the-badge&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Tailwind CSS](https://img.shields.io/badge/Tailwind_CSS-v4-38B2AC?style=for-the-badge&logo=tailwind-css&logoColor=white)](https://tailwindcss.com/)
[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg?style=for-the-badge)](https://opensource.org/licenses/Apache-2.0)

---

## 📌 Table of Contents

- [Executive Summary](#-executive-summary)
- [System Architecture](#-system-architecture)
- [Key Features](#-key-features)
- [Technology Stack](#-technology-stack)
- [Project Directory Structure](#-project-directory-structure)
- [Prerequisites](#-prerequisites)
- [Getting Started](#-getting-started)
  - [1. Clone Repository](#1-clone-repository)
  - [2. Install Node Dependencies](#2-install-node-dependencies)
  - [3. Configure Environment](#3-configure-environment)
  - [4. Start Web Control Plane](#4-start-web-control-plane)
  - [5. Run Python Distributed Engine (Optional)](#5-run-python-distributed-engine-optional)
- [Environment Variables](#-environment-variables)
- [Development & Build Scripts](#-development--build-scripts)
- [Security & Best Practices](#-security--best-practices)
- [Project Status & Roadmap](#-project-status--roadmap)
- [Contributors & Team](#-contributors--team)
- [License](#-license)

---

## 🚀 Executive Summary

**StreamForge** (also referenced as **SteamForge**) is an enterprise-grade distributed stateful stream processing engine paired with a real-time observability and chaos engineering control plane. 

Engineered to process continuous, high-frequency IoT telemetry from **50,000 cold-chain refrigerated transport vehicles**, the system combines:
1. **A Pure Python Distributed Streaming Backend**: Featuring 32 Kafka partitions, 5-minute tumbling/rolling windows with online Welford statistics, embedded RocksDB LSM-tree state stores with Write-Ahead Log (WAL) replication, cooperative sticky partition rebalancing, and Prometheus metrics export.
2. **A Modern High-Performance Web Cockpit**: Built with React 18, Vite, TypeScript, and Tailwind CSS, providing interactive topology monitoring, partition range spectrum ribbons, live consumer lag inspection, and one-click chaos fault injection.

---

## 📐 System Architecture

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                       50,000 IoT Connected Vehicles                         │
│             Continuous GPS, Temperature, Humidity, Compressor Telemetry      │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼ Murmur2 Hashing (Key = truck_id)
┌─────────────────────────────────────────────────────────────────────────────┐
│                 Apache Kafka Topic: fleet-telemetry (32 Partitions)         │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼ Cooperative Sticky Consumer Group
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Distributed Worker Node Cluster (20 Nodes)              │
│                                                                             │
│  ┌───────────────────┐  ┌───────────────────┐  ┌─────────────────────────┐  │
│  │   Worker Node 01  │  │   Worker Node 02  │  │     Worker Node 04      │  │
│  │   Partitions 0-7  │  │   Partitions 8-15 │  │    Partitions 24-31     │  │
│  │  ┌─────────────┐  │  │  ┌─────────────┐  │  │                         │  │
│  │  │ 5-Min Window│  │  │  │ 5-Min Window│  │  │   [CHAOS FAULT ZONE]    │  │
│  │  │ Engine (O1) │  │  │  │ Engine (O1) │  │  │   Worker Kill / SIGKILL │  │
│  │  └──────┬──────┘  │  │  └──────┬──────┘  │  │                         │  │
│  │  ┌──────▼──────┐  │  │  ┌──────▼──────┐  │  │                         │  │
│  │  │   RocksDB   │  │  │  │   RocksDB   │  │  │   Rebalance & Recovery  │  │
│  │  │  LSM-Store  │  │  │  │  LSM-Store  │  │  │   Automatic Partition   │  │
│  │  └──────┬──────┘  │  │  └──────┬──────┘  │  │   Failover Migration    │  │
│  └─────────┼─────────┘  └─────────┼─────────┘  └────────────┬────────────┘  │
└────────────┼──────────────────────┼─────────────────────────┼───────────────┘
             │                      │                         │
             ▼                      ▼                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│       Kafka Changelog Compacted Topic (Write-Ahead Log Mirror for RocksDB)   │
│             Guarantees Zero Data Loss: RPO = 0, RTO < 50ms State Replay     │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     StreamForge Web Control Plane & Cockpit                 │
│         React 18 • Vite • Kafka Partition Visualizer • Prometheus Metrics   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## ✨ Key Features

### 1. 🖥️ Real-Time Kafka Partition Visualizer
- **Partition Range Spectrum Ribbon**: Color-coded 32-partition spectrum bar with contiguous range brackets (`P00–P01`, `P02–P03`, etc.).
- **Worker Allocation Matrix**: Displays each worker node's partition assignments, aggregate consumer lag, and ingestion rate.
- **Deep Partition Inspector**: Inspect log end offsets (LEO), committed consumer offsets, high watermarks, broker rack locations, and RocksDB state keys.

### 2. ⚡ High-Throughput Windowing & Statistics
- **5-Minute Rolling Aggregations**: Continuous stream windowing using Welford’s online algorithm for $O(1)$ constant-memory mean and variance calculation.
- **Event-Time Watermarks**: Automatic handling of out-of-order and late-arriving telemetry with configurable grace periods.

### 3. 🗄️ Embedded RocksDB State Store
- **LSM-Tree Engine**: Local in-memory MemTables, level-compacted SSTable files, and changelog replication.
- **Zero-Data-Loss Failover**: Replays Kafka changelog mutations automatically upon partition migration ($RPO = 0$, $RTO < 50\text{ms}$).

### 4. 💥 Chaos Engineering Studio
- One-click worker crashes (SIGKILL emulation) to validate cooperative sticky rebalancing.
- Simulates network partitioning, CPU throttling, and sensor drift with instant visual verification.

### 5. 📊 Enterprise Telemetry & Prometheus Daemon
- Live metrics dashboard tracking p50, p95, and p99 processing latencies, heap memory utilization, and throughput.
- Standard `/metrics` Prometheus scraper format.

---

## 🛠️ Technology Stack

| Domain | Technology | Description |
| :--- | :--- | :--- |
| **Frontend Framework** | **React 18** | High-performance reactive UI with modular component architecture |
| **Build Tool** | **Vite 6** | Instant Hot-Module-Replacement and optimized production bundler |
| **Language** | **TypeScript 5** | Strict static type checking, interfaces, and end-to-end type safety |
| **Styling** | **Tailwind CSS v4** | Modern utility-first CSS framework with dark-mode aesthetic |
| **Icons** | **Lucide React** | Clean, accessible SVG vector icon system |
| **State Storage** | **RocksDB** | Embedded LSM-tree storage engine with Write-Ahead Logging (WAL) |
| **Stream Broker** | **Apache Kafka** | Distributed commit log with 32 partitions and sticky rebalancing |
| **Backend Engine** | **Python 3.9+** | Object-oriented PEP 8 stream engine, Pydantic v2 schemas |
| **Monitoring** | **Prometheus** | Real-time time-series telemetry and metric exposition |

---

## 📁 Project Directory Structure

```text
StreamForge/
├── public/                          # Public static assets & favicon
│   └── vite.svg
├── src/                             # Web application source code
│   ├── components/                  # Modular UI components
│   │   ├── ChaosStudio.tsx          # Chaos engineering injection cockpit
│   │   ├── CodebaseExplorer.tsx     # Full Python engine source code viewer
│   │   ├── FleetMonitor.tsx         # 50,000 IoT vehicle fleet tracker
│   │   ├── KafkaPartitionVisualizer.tsx # Live partition range & allocation visualizer
│   │   ├── Member1Handbook.tsx      # Architecture handbook & technical defense
│   │   ├── MetricsDashboard.tsx     # Prometheus p99 latency & throughput stats
│   │   ├── Navbar.tsx               # Top navigation & real-time KPI bar
│   │   ├── RocksDBInspector.tsx     # LSM-Tree, MemTable & SSTable file inspector
│   │   ├── TopologyView.tsx         # Cluster nodes & partition mapping
│   │   └── WindowingLab.tsx         # Interactive 5-min rolling window lab
│   ├── data/                        # Static datasets & curriculum guides
│   ├── engine/                      # Real-time simulation engine
│   │   └── simulationEngine.ts
│   ├── types/                       # TypeScript interfaces & domain models
│   │   └── stream.ts
│   ├── App.tsx                      # Root application layout
│   ├── index.css                    # Tailwind CSS v4 entry point
│   └── main.tsx                     # React DOM entry point
├── streamforge/                     # Distributed Python streaming engine
│   ├── core/                        # Pydantic models & ABC interfaces
│   ├── windowing/                   # 5-min rolling window processor
│   ├── state/                       # RocksDB state store & changelog manager
│   ├── recovery/                    # Cooperative sticky rebalancer
│   ├── producers/                   # 50,000 IoT synthetic telemetry generator
│   └── metrics/                     # Prometheus exposition daemon
├── tests/                           # Python unit, integration & chaos tests
├── main.py                          # Python CLI runner (live, benchmark, chaos)
├── requirements.txt                 # Python dependencies
├── package.json                     # Node dependencies & npm scripts
├── vite.config.ts                   # Vite build configuration
└── README.md                        # Project documentation
```

---

## 📦 Prerequisites

Before running the project, ensure you have the following installed:

- **Node.js**: `v18.0.0` or higher ([Download Node.js](https://nodejs.org/))
- **npm**: `v9.0.0` or higher (comes bundled with Node.js)
- **Python** *(Optional, for standalone Python CLI)*: `Python 3.9+` ([Download Python](https://www.python.org/))

Verify your local installation:

```bash
node --version
npm --version
python3 --version
```

---

## 🚀 Getting Started

### 1. Clone Repository

```bash
git clone https://github.com/your-username/StreamForge.git
cd StreamForge
```

### 2. Install Node Dependencies

```bash
npm install
```

### 3. Configure Environment

Create a `.env` or `.env.local` file in the project root:

```bash
cp .env.example .env.local
```

Populate the required keys (see [Environment Variables](#-environment-variables) below).

### 4. Start Web Control Plane

Run the Vite development server:

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) (or the URL shown in your terminal) in your browser.

### 5. Run Python Distributed Engine (Optional)

If running the Python streaming engine backend locally:

```bash
# Set up a virtual environment
python3 -m venv venv
source venv/bin/activate    # On Windows: venv\Scripts\activate

# Install Python requirements
pip install -r requirements.txt

# Run benchmark test (100,000 events)
python3 main.py --mode=benchmark --events=100000

# Run chaos test demo
python3 main.py --mode=chaos

# Run test suite
python3 -m pytest tests/ -v
```

---

## 🔑 Environment Variables

| Variable | Required | Default | Description |
| :--- | :---: | :---: | :--- |
| `GEMINI_API_KEY` | Optional | `""` | Google Gemini API key used for AI-assisted streaming analysis |
| `PORT` | Optional | `3000` | Port used by the local preview server |
| `NODE_ENV` | Optional | `development` | Runtime environment (`development` or `production`) |

---

## 🧪 Development & Build Scripts

| Command | Purpose |
| :--- | :--- |
| `npm run dev` | Starts the Vite development server with instant HMR |
| `npm run build` | Compiles TypeScript and builds optimized production bundle to `dist/` |
| `npm run preview` | Locally previews the production build output from `dist/` |
| `npm run lint` | Runs TypeScript compiler checks (`tsc --noEmit`) to validate type safety |

---

## 🔒 Security & Best Practices

- **Never Commit Secrets**: Keep all credentials, tokens, and API keys strictly inside `.env` or `.env.local`.
- **Gitignore Protection**: Ensure sensitive files are excluded from version control.
  ```text
  # .gitignore
  .env
  .env.local
  .env.*.local
  node_modules/
  dist/
  __pycache__/
  *.pyc
  .pytest_cache/
  ```
- **Server-Side API Safeguards**: Sensitive API keys (e.g. `GEMINI_API_KEY`) are accessed securely server-side and never exposed to client-side browser bundles.

---

## 📌 Project Status & Roadmap

- **Status**: 🟢 **Active / Production-Ready Architecture**
- **Completed Milestones**:
  - [x] 32-Partition Kafka consumer group emulation with Murmur2 hashing
  - [x] 5-minute tumbling and rolling window statistical engine ($O(1)$ memory)
  - [x] Embedded RocksDB LSM-Tree store with Write-Ahead-Log changelog mirror
  - [x] Real-time Kafka partition visualizer with contiguous range brackets
  - [x] Interactive chaos engineering crash & failover validation
  - [x] Prometheus metrics daemon and live telemetry cockpit
- **Upcoming Work**:
  - [ ] Multi-region active-active cluster replication
  - [ ] Exact-once processing semantics (EOS) with two-phase commit
  - [ ] gRPC streaming telemetry ingestion proxy

---

## 👥 Contributors & Team

Developed collaboratively as part of an engineering internship & distributed systems capstone team:

- **Member 1**: Core Stream Processing Engine, RocksDB State Store, 5-Minute Rolling Window Aggregator, Cooperative Sticky Partition Rebalancing, and Prometheus Metrics Exporter.
- **Control Plane & UI Team**: Real-time Kafka Partition Visualizer, Chaos Engineering Cockpit, and Observability Dashboard.

---

## 📄 License

This project is licensed under the **Apache License 2.0**. See the [LICENSE](LICENSE) file for more details.

---

<div align="center">
  <sub>Built with craftsmanship for high-throughput distributed systems. If you find this project valuable, consider starring the repository! ⭐</sub>
</div>
