# qrp-atlas

**Market Deconstruction Toolkit for Quantitative Analysis**

A personal toolkit for quantitative analysis and market review, designed to deconstruct market structures into clear, observable components — without automated trading.

[![Python 3.13](https://img.shields.io/badge/Python-3.13-blue.svg)](https://www.python.org/downloads/)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.10+-orange.svg)](https://duckdb.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.0+-red.svg)](https://streamlit.io/)

---

## Overview

**qrp-atlas** is a research-focused tool built around **data infrastructure + visual market review**.

### Capabilities

| Scope | Implementation |
|-------|---------------|
| Market Data Processing | Automated ingestion from EastMoney sources |
| Quantitative Analysis | Daily bar canonicalization and enrichment |
| Visual Review Interface | Streamlit-based interactive dashboards |

### Out of Scope

- Automated trading execution
- Strategy backtesting frameworks
- Trading signal generation

> **Philosophy**: Machine handles structure. Human handles narrative.

---

## Architecture

```
Data Flow: daily_snapshot → ingestion → DuckDB

Key Principles:
- Reusable components live in src/
- One-time scripts confined to scripts/
- Local data (data/) excluded from version control
```

### Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Language | Python 3.13 | Core implementation |
| Database | DuckDB | OLAP analytical queries |
| Data Processing | Pandas | DataFrame operations |
| Visualization | Streamlit | Interactive web interface |
| Data Source | EastMoney API | Market data ingestion |

### Project Structure

```
qrp-atlas/
├── src/qrp_atlas/          # Core library modules
│   ├── config/             # Configuration management (SSOT)
│   ├── contracts/          # Database schema definitions (SSOT)
│   ├── pipeline/           # Data processing pipelines
│   │   └── daily_update/   # Daily update workflow
│   └── sources/            # External data source adapters
├── scripts/                # One-time utility scripts
├── web/                    # Streamlit application
│   └── pages/              # Multi-page navigation
├── docs/                   # Architecture documentation
├── data/                   # Local DuckDB storage (gitignored)
└── pyproject.toml          # Project metadata
```

---

## Data Pipeline

### Daily Update Workflow

1. **Fetch**: Retrieve daily snapshot from EastMoney API
2. **Clean**: Standardize field names and formats
3. **Enrich**: Cross-reference with existing canonical data
4. **Load**: Persist to DuckDB for analytical queries

### Data Conventions

- All table schemas defined in `qrp_atlas.contracts`
- All filesystem paths managed via `qrp_atlas.config`
- No schema modifications without contract updates

---

## Development Status

**Early Development** — Core infrastructure in progress

Current milestones:
- [x] Data ingestion from EastMoney
- [x] DuckDB storage layer
- [x] Daily update pipeline
- [x] Basic Streamlit interface
- [ ] Advanced visualization features
- [ ] Extended analytical tools

---

## Getting Started

### Prerequisites

- Python 3.13+
- DuckDB

### Installation

```bash
pip install -e .
```

### Configuration

Copy `.env.example` to `.env` and configure data source credentials if required.

### Running the Application

```bash
streamlit run web/app.py
```

---

## License

Personal project. All rights reserved.

---

*Last updated: 2026-05-04*
