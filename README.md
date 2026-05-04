# qrp-atlas

**Market Deconstruction Toolkit — AI-Augmented Quant Analysis & Daily Recon**

A human-in-the-loop research platform that deconstructs the market into clean, observable structures. Designed for the analyst who believes machines handle the data while humans own the narrative.

[![Python 3.13](https://img.shields.io/badge/Python-3.13-blue.svg)](https://www.python.org/downloads/)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.10+-orange.svg)](https://duckdb.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.0+-red.svg)](https://streamlit.io/)
[![AI Pipeline](https://img.shields.io/badge/AI–Ready-OpenAI%20Compatible-blue.svg)]()

---

## Overview

**qrp-atlas** reimagines the daily trading workflow as a **human–AI collaborative cockpit**. It is not an automated trading system — it is a *deconstruction engine* that ingests raw market data, enriches it with computed intelligence, and surfaces it through an interactive interface where the human analyst makes the final call.

### Core Philosophy

```
Market Data → Machine Processing → Structured Intelligence → Human Judgement → Decision
```

We build the pipeline; you bring the narrative.

### What It Does

| Capability | Implementation |
|-----------|---------------|
| Multi-Source Data Ingestion | Automatic failover across tushare / Sina / East Money |
| Daily Market Intelligence | Raw → Cleaned → Enriched → Indexed pipeline |
| Interactive Visualization | Streamlit dashboard with charting & screening |
| Human-in-the-Loop Design | Every output is a *proposition*, not a decision |

### What It Is Not

- ❌ An auto-trading bot
- ❌ A black-box signal generator
- ❌ A backtesting framework

> **The machine structures. The human decides.**

---

## Human–AI Interaction Design

qrp-atlas is built from the ground up around a **collaborative intelligence** model:

### Phase 1 — Machine Does the Grunt Work
- Automated multi-source data collection with intelligent failover
- Schema-enforced cleaning and canonicalization
- Computed enrichments: limit-up/down detection, ST classification, sector-aware P&L
- All data lands in a structured, query-optimized DuckDB store — the **single source of truth** for the day's session

### Phase 2 — Analyst Takes the Helm
- Interactive Streamlit dashboards expose the structured intelligence
- The analyst navigates, filters, contextualizes, and interprets
- Visual patterns trigger human insight — the machine *shows*, the human *sees*

### Phase 3 — AI-Augmented Exploration *(Roadmap)*
- Natural-language query interface against the DuckDB store
- AI-suggested anomaly detection (volume spikes, price gaps, sector rotation signals)
- Conversational drill-down: ask the system "what moved in semis today?" and get a structured answer

This three-phase loop ensures the human remains the locus of judgment while the machine handles scale, speed, and structure.

---

## Multi-Agent Collaboration Vision

We are actively designing a **multi-agent orchestration layer** that transforms qrp-atlas from a single-pipeline tool into a **collaborative analyst swarm**. Each agent is a specialized reasoning unit, coordinated through a shared memory layer (DuckDB) and a human supervisory loop.

### Planned Agent Roles

| Agent | Responsibility | Interface |
|-------|---------------|-----------|
| **Sentinel Agent** | Real-time market anomaly detection: unusual volume, price gaps, limit-up clusters | Streamlit alert feed + notification |
| **Sector Rotator Agent** | Tracks capital flows across industry groups, flags rotation signals | Dashboard overlay + summary card |
| **Narrative Agent** | Summarizes cross-market activity into natural-language daily briefs | AI-generated morning/evening report |
| **Integrity Agent** | Validates pipeline health: data completeness, schema compliance, source freshness | Status panel + alert on failure |
| **Orchestrator Agent** | Routes tasks, manages inter-agent context, escalates to human when confidence is low | Central coordination hub |

### Coordination Model

```
                         ┌─────────────────┐
                         │   Orchestrator   │
                         │      Agent       │
                         └────────┬────────┘
                                  │
          ┌───────────────┬───────┼───────┬───────────────┐
          │               │       │       │               │
    ┌─────▼─────┐  ┌─────▼────┐ ┌▼────┐ ┌▼────────┐ ┌───▼────────┐
    │  Sentinel │  │  Sector  │ │Narra│ │Integrity│ │   Human    │
    │   Agent   │  │ Rotator  │ │tive │ │  Agent  │ │  Overseer  │
    └─────┬─────┘  └─────┬────┘ └──┬──┘ └──┬───────┘ └───┬────────┘
          │               │        │        │             │
          └───────────────┴────────┴────────┴─────────────┘
                                  │
                        ┌─────────▼─────────┐
                        │   Shared Memory   │
                        │   (DuckDB Store)  │
                        └───────────────────┘
```

### Key Design Principles

- **Human-overridden, never overridden-by** — Every agent output is a proposal; the human can accept, reject, or modify before any action
- **Shared fact layer** — All agents read from and write to the same DuckDB schema, ensuring a consistent world model
- **Bounded autonomy** — Agents operate within well-defined scopes; cross-boundary decisions escalate to the human
- **Observability by default** — Every agent's reasoning trace is logged and inspectable through the Streamlit interface

This architecture is under active research and prototyping. Our goal is to ship the **Sentinel Agent** and **Integrity Agent** as the first production-ready modules, providing immediate value for daily monitoring workflows.

---

## Technology Stack

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Language | Python 3.13 | Ecosystem depth for data & AI |
| Database | DuckDB | OLAP-optimized, embedded, zero-config |
| Data Processing | Pandas | Universal DataFrame interface |
| Visualization | Streamlit | Fastest path from data to interactive UI |
| Data Sources | akshare + tushare | Multi-source A-share coverage with auto-failover |
| AI Inference (Planned) | OpenAI-compatible API | Flexible LLM backend for agent reasoning |

### Project Structure

```
qrp-atlas/
├── src/qrp_atlas/          # Core library
│   ├── config/             # Path & settings SSOT
│   ├── contracts/          # Schema definitions (field names, table specs, mappings)
│   ├── pipeline/           # Data processing pipeline
│   │   └── daily_update/   # Daily market snapshot workflow
│   └── sources/            # External data adapters
├── web/                    # Streamlit application
│   ├── app.py              # Entry point
│   └── pages/              # Multi-page navigation
├── docs/                   # Architecture documentation
├── data/                   # Local DuckDB storage (gitignored)
└── pyproject.toml          # Project metadata
```

---

## Data Pipeline

### Daily Update Workflow

```
Fetch ──▶ Clean ──▶ Enrich ──▶ Load ──▶ Visualize
  │          │          │          │
  ▼          ▼          ▼          ▼
 Raw CSV   Cleaned   Canonical   DuckDB
 (backup)   CSV       CSV         (runtime
            (audit)   (recovery   SSOT)
                       SSOT)
```

1. **Fetch** — Multi-source auto-failover (tushare → Sina akshare → East Money akshare). Automatic trading-day detection with post-15:00 CST cutoff.
2. **Clean** — Schema-enforced column mapping via `contracts.mappings`, type canonicalization, deduplication.
3. **Enrich** — Cross-reference with prior session, compute P&L, detect limit-up/down, classify ST status.
4. **Load** — Transactional upsert into DuckDB with rollback on failure. Every write passes `quick_validate()`.

### Data Contracts (SSOT)

| Contract Layer | Location | Purpose |
|----------------|----------|---------|
| Path SSOT | `config/paths.py` | All filesystem paths defined once |
| Field SSOT | `contracts/fields.py` | All column name constants |
| Table SSOT | `contracts/schema.py` | All table structures (18-column `daily_market_snapshot`, plus `market_phase` and `trade_execution`) |
| Mapping SSOT | `contracts/mappings.py` | Source-to-canonical field mappings for 6 source types |
| Validation | `contracts/validate.py` | `quick_validate()` enforces schema before every write |

---

## Development Roadmap

### Current Milestone — Core Pipeline (Complete)
- [x] Multi-source data ingestion with auto-failover
- [x] DuckDB storage layer with transactional writes
- [x] Daily update pipeline (fetch → clean → enrich → load)
- [x] Schema-enforced data contracts & validation
- [x] Basic Streamlit page framework

### Next Milestone — Interactive Visualization (In Progress)
- [ ] Individual stock K-line with MA overlays
- [ ] Daily market-wide overview with screening & filtering
- [ ] Sector/industry grouping and relative strength visualization
- [ ] Limit-up/down heatmap

### Future Milestone — Multi-Agent Intelligence
- [ ] Sentinel Agent — real-time anomaly detection
- [ ] Integrity Agent — pipeline health monitoring
- [ ] Narrative Agent — AI-generated daily market brief
- [ ] Natural-language query interface against DuckDB
- [ ] Multi-agent coordination dashboard

---

## Quick Start

### Prerequisites

- Python 3.13+
- DuckDB (bundled via Python package)

### Installation

```bash
pip install -e .
```

### Configuration

Copy `.env.example` to `.env` and configure data source credentials (tushare token, etc.).

### Run

```bash
streamlit run web/app.py
```

### Daily Pipeline

```bash
python -m qrp_atlas.pipeline.daily_update.run
```

---

## License

Personal project. All rights reserved.

---

*Last updated: 2026-05-04*


