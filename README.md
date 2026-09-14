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
[50,000 IoT Trucks] 
       │ 
       ▼ Murmur2 Partition Hashing (Key = truck_id)
[Kafka Topic: fleet-telemetry (32 Partitions)]
       │
       ▼ Cooperative Sticky Rebalancer
┌─────────────────────────────────────────────────────────────┐
│                 StreamForge Cluster (20 Nodes)              │
│                                                             │
│  ┌───────────────┐ ┌───────────────┐ ┌───────────────────┐  │
│  │   Worker 01   │ │   Worker 02   │ │     Worker 04     │  │
│  │  (Parts 0-7)  │ │  (Parts 8-15) │ │   (Parts 24-31)   │  │
│  │               │ │               │ │   [CRASH CHAOS]   │  │
│  │ ┌───────────┐ │ │ ┌───────────┐ │ │                   │  │
│  │ │ 5-Min Win │ │ │ │ 5-Min Win │ │ │                   │  │
│  │ └─────┬─────┘ │ │ └─────┬─────┘ │ │                   │  │
│  │ ┌─────▼─────┐ │ │ ┌─────▼─────┐ │ │                   │  │
│  │ │  RocksDB  │ │ │ │  RocksDB  │ │ │                   │  │
│  │ │ StateStore│ │ │ │ StateStore│ │ │                   │  │
│  │ └─────┬─────┘ │ │ └─────┬─────┘ │ │                   │  │
│  └───────┼───────┘ └───────┼───────┘ └─────────┬─────────┘  │
└──────────┼─────────────────┼───────────────────┼────────────┘
           │                 │                   │
           ▼                 ▼                   ▼
┌─────────────────────────────────────────────────────────────┐
│   Kafka Changelog Compacted Topic (WAL Mirror for RocksDB)  │
│   --> Guarantees RPO = 0, RTO < 50ms State Replay           │
└─────────────────────────────────────────────────────────────┘
```

---

## ✨ Key Features

- **Real-Time Kafka Partition Visualizer**: Color-coded 32-partition spectrum bar with contiguous range brackets (P00–P01, P02–P03, etc.).
- **5-Minute Rolling Aggregations**: Tumbling and sliding window calculations using online Welford algorithms for O(1) memory consumption.
- **Embedded RocksDB State Store**: Fast local LSM-Tree store with in-memory MemTables, SSTable layers, and Write-Ahead Log (WAL) sync.
- **Zero-Data-Loss Failover**: Automatic partition ownership shift and Kafka changelog mutation replay (RPO = 0, RTO < 50ms).
- **Chaos Engineering Studio**: Live worker crash simulation, partition rebalance verification, and disaster recovery validation.
- **Prometheus Metrics Daemon**: Native exposition endpoint tracking p50/p95/p99 latency, consumer lag, and ingestion throughput.

---

## 🛠️ Technology Stack

| Domain | Technology | Description |
| :--- | :--- | :--- |
| **Frontend Framework** | **React 18** | High-performance reactive UI with modular component architecture |
| **Build Tool** | **Vite 6** | Instant Hot-Module-Replacement and optimized production bundler |
| **Language** | **TypeScript 5** | Strict static type checking, interfaces, and end-to-end type safety |
| **Styling** | **Tailwind CSS v4** | Modern utility-first CSS framework with dark-mode aesthetic |
| **State Storage** | **RocksDB** | Embedded LSM-tree storage engine with Write-Ahead Logging (WAL) |
| **Stream Broker** | **Apache Kafka** | Distributed commit log with 32 partitions and sticky rebalancing |
| **Backend Engine** | **Python 3.9+** | Object-oriented PEP 8 stream engine, Pydantic v2 schemas |
| **Monitoring** | **Prometheus** | Real-time time-series telemetry and metric exposition |

---

## 📦 Getting Started

```bash
# 1. Clone repository
git clone https://github.com/your-username/StreamForge.git
cd StreamForge

# 2. Install dependencies
npm install

# 3. Start development server
npm run dev

# 4. Build for production
npm run build
```

---

## 🔑 Environment Variables

| Variable | Required | Description |
| :--- | :---: | :--- |
| `GEMINI_API_KEY` | Optional | API key used for AI-assisted streaming telemetry analysis |
| `PORT` | Optional | Local development port (defaults to 3000) |
| `NODE_ENV` | Optional | Runtime environment (`development` or `production`) |

---

## 📄 License

This project is licensed under the **Apache License 2.0**.
