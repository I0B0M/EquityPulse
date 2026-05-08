# EquityPulse

A real-time stock risk and portfolio intelligence platform I built to learn the kind of backend systems that actually matter in fintech — live data pipelines, time-series storage, institutional-grade risk math, and a dashboard that updates without you refreshing the page.

Not a tutorial project. Every design decision mirrors what you'd find at a fund, a prime broker, or a trading desk — and I can explain all of it.

---

## What it does

Connects to the Polygon.io WebSocket feed, streams live NYSE/NASDAQ tick data through Kafka, stores it in TimescaleDB (a purpose-built time-series database), runs an institutional risk engine on top, and serves everything through a FastAPI backend with a React dashboard that updates in real time.

**Live dashboard:** prices for 10 tickers update via WebSocket. Risk metrics (VaR, Sharpe, volatility, drawdown) refresh every 30 seconds. The efficient frontier chart recomputes using 10,000 Monte Carlo simulations whenever you load it.

---

## Tech decisions I made (and why)

**Kafka over direct DB writes** — The ingestion service and the storage layer are decoupled. If the DB is slow, the producer doesn't block. If I want to add a second consumer (e.g. a separate alerts service), I don't touch the producer. That's the right architecture when you're handling thousands of events per minute.

**TimescaleDB over plain Postgres** — Hypertables partition time-series data into weekly chunks automatically. Range queries on `price_bars` are 10-100x faster than the same table in vanilla Postgres because the query planner only scans relevant chunks. I also built a continuous aggregate (`price_bars_1min`) that TimescaleDB refreshes every minute — minute-bar queries are instant.

**Historical Simulation VaR over Parametric** — Real market returns have fat tails. A normal distribution would underestimate tail risk by 20-40% in volatile regimes. Historical simulation makes zero assumptions about the distribution shape and naturally captures the skewness and kurtosis from actual price data.

**Sortino over Sharpe for asymmetric strategies** — Sharpe penalises upside volatility, which is nonsensical. A strategy that goes up sharply but has small drawdowns looks worse by Sharpe than it deserves. Sortino only penalises downside deviation.

**SciPy SLSQP for MPT** — The analytical solution to Markowitz optimisation only works without constraints. Real portfolios have long-only constraints, position limits, etc. SLSQP handles all of that as linear inequality constraints — same solver used in institutional PM systems.

**Redis as the hot cache** — The dashboard needs current prices for 10 symbols simultaneously. A TimescaleDB SELECT is ~5ms; a Redis GET is ~0.1ms. For a live dashboard showing P&L updating on every tick, that 50x difference is visible. The risk engine writes to Redis; the API reads from it. No recomputation at request time.

---

## Architecture

```
Polygon.io WebSocket
       │
       ▼
  ingestion/            ← connects to WS, publishes to Kafka
       │
  Apache Kafka          ← raw.ticks + aggregated.ohlcv topics
       │
  storage/consumer      ← reads Kafka, writes to TimescaleDB + Redis
       │          │
 TimescaleDB    Redis   ← hypertable for history, hot cache for live prices
       │          │
  risk/engine           ← reads prices, computes VaR/vol/Sharpe, writes back to Redis
       │
  api/ (FastAPI)        ← REST endpoints + WebSocket broadcaster
       │
  frontend/ (React)     ← live dashboard, updates via WebSocket
```

---

## Risk engine — what's actually computed

All of this is implemented from scratch in NumPy. No finance libraries.

| Metric | Method | Why this method |
|---|---|---|
| VaR 95% / 99% | Historical Simulation | Fat tails. Real returns aren't normal. |
| Rolling Volatility | 20-day and 60-day annualised σ×√252 | Two windows to detect expanding/contracting vol regimes |
| Sharpe Ratio | (R − Rf) / σ_total, Rf = 5.3% (Fed funds) | Excess return per unit of total risk |
| Sortino Ratio | (R − Rf) / σ_downside | Same but only penalises negative returns |
| Max Drawdown | Running max accumulation | Peak-to-trough decline — the metric PMs actually care about |

Alert thresholds: VaR 95% > 3%, annualised vol > 40%, Sharpe < 0, drawdown < −15%.

---

## Portfolio engine

- **Monte Carlo frontier**: 10,000 random weight combinations, each evaluated for return/vol/Sharpe. The resulting scatter plot *is* the efficient frontier visualisation.
- **Minimum variance**: SciPy SLSQP minimises `wᵀΣw` subject to `Σwᵢ = 1` and `wᵢ ≥ 0` (long-only constraint).
- **Maximum Sharpe** (tangency portfolio): minimises negative Sharpe via the same solver.
- **Drift-based rebalancing**: flags positions that have drifted >5% from target weight and generates BUY/SELL orders sorted sells-first (so you have cash before buying).

---

## Project structure

```
EquityPulse/
├── ingestion/                 # Polygon WebSocket client + Kafka producer
│   ├── polygon_client.py      # WS connection, auth, subscribe, reconnect backoff
│   └── kafka_producer.py      # LZ4 compression, batching, throughput metrics
│
├── storage/                   # Persistence layer
│   ├── schema.sql             # TimescaleDB hypertable + continuous aggregate
│   ├── timescale_client.py    # Batch inserts, connection pool, query methods
│   ├── redis_client.py        # Live price cache, ring buffer, risk cache
│   └── consumer.py            # Kafka → TimescaleDB + Redis bridge
│
├── risk/                      # Risk computation engine
│   ├── metrics.py             # VaR, volatility, Sharpe, Sortino, drawdown (pure NumPy)
│   ├── engine.py              # Continuous process: reads prices, writes metrics to Redis
│   └── test_metrics.py        # 7 unit tests with synthetic GBM price data
│
├── portfolio/                 # Portfolio analytics
│   ├── optimizer.py           # MPT: Monte Carlo frontier + SLSQP min-var + max-Sharpe
│   ├── rebalancer.py          # Drift detection, order generation, P&L attribution
│   └── test_portfolio.py      # 9 unit tests
│
├── api/                       # FastAPI backend
│   ├── main.py                # App factory, CORS, Prometheus, WebSocket endpoint
│   ├── models.py              # Pydantic response models (drives Swagger docs)
│   ├── deps.py                # Dependency injection (shared Redis/TimescaleDB pools)
│   ├── websocket_manager.py   # Connection manager + async price broadcaster
│   └── routes/
│       ├── prices.py          # GET /api/prices/{symbol} + /history
│       ├── risk.py            # GET /api/risk/{symbol} + /portfolio/aggregate
│       ├── portfolio.py       # GET /api/portfolio, /frontier, POST /rebalance
│       └── health.py          # GET /api/health
│
├── frontend/                  # React dashboard (Vite + TypeScript)
│   └── src/
│       ├── hooks/             # useWebSocket (with reconnect), useApi (polling)
│       └── components/        # PriceGrid, RiskPanel, PortfolioPanel, FrontierChart, AlertBanner
│
├── nginx/                     # Reverse proxy config (prod)
├── .github/workflows/ci.yml   # CI: Python tests + TS type-check + Docker build
├── docker-compose.yml         # Dev: Kafka + TimescaleDB + Redis
└── docker-compose.prod.yml    # Prod: adds all Python services + Nginx
```

---

## Running it locally

**Prerequisites:** Docker Desktop, Python 3.12, Node 20

```bash
git clone https://github.com/I0B0M/EquityPulse.git
cd EquityPulse

# 1. Spin up infrastructure
docker compose up -d

# 2. Python environment
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pip install fastapi==0.111.0 "uvicorn[standard]==0.29.0" scipy==1.13.0 prometheus-client==0.20.0

# 3. Copy env and add your Polygon API key (free tier works)
cp .env.example .env
# edit .env → POLYGON_API_KEY=your_key_here

# 4. Initialise the database
python -m storage.init_db

# 5. Run all services (during market hours: 9:30am–4pm ET weekdays)
python -m ingestion.main      # Terminal 1
python -m storage.consumer    # Terminal 2
python -m risk.engine         # Terminal 3
uvicorn api.main:app --port 8000  # Terminal 4

# 6. Frontend
cd frontend && npm install && npm run dev
# → http://localhost:5173
```

API docs auto-generated at **http://localhost:8000/docs**

---

## API endpoints

| Method | Endpoint | What it returns |
|---|---|---|
| `GET` | `/api/prices/{symbol}` | Latest price from Redis (~0.1ms) |
| `GET` | `/api/prices/{symbol}/history` | OHLCV bars from TimescaleDB |
| `GET` | `/api/risk/{symbol}` | Pre-computed risk metrics from Redis cache |
| `GET` | `/api/risk/portfolio/aggregate` | All 10 symbols in one call + active alerts |
| `GET` | `/api/portfolio` | Positions, weights, live P&L |
| `GET` | `/api/portfolio/frontier` | 10,000-point Monte Carlo scatter + optimal portfolios |
| `POST` | `/api/portfolio/rebalance` | Drift analysis + BUY/SELL order list |
| `GET` | `/api/health` | Redis + TimescaleDB status + latency |
| `WS` | `/ws/live` | Live price stream (1s updates) + risk alerts |
| `GET` | `/metrics` | Prometheus scrape endpoint |

---

## Running the tests

```bash
# Risk engine — 7 tests
python -m risk.test_metrics

# Portfolio engine — 9 tests
python -m portfolio.test_portfolio

# Frontend types
cd frontend && npx tsc --noEmit
```

All tests use synthetic price data generated via Geometric Brownian Motion — no live services needed.

---

## Production deployment (AWS EC2)

```bash
# On EC2 t3.medium (Ubuntu 22.04)
git clone https://github.com/I0B0M/EquityPulse.git && cd EquityPulse

# Build the React app first
cd frontend && npm ci && npm run build && cd ..

# Set production secrets
cp .env.example .env.prod
# edit .env.prod with real credentials

# Start everything
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

Nginx serves the React build statically and proxies `/api/*` and `/ws/*` to the FastAPI container. WebSocket upgrades are handled with the correct `Upgrade: websocket` headers.

---

## What I'd add next

- **Prometheus + Grafana dashboards** — the `/metrics` endpoint is already wired; just needs the Grafana config
- **Kafka exactly-once semantics** — currently at-least-once; adding idempotent producers and transactional consumers would make the pipeline production-grade
- **Multi-period backtesting** — run the risk metrics on historical windows to see how VaR predictions held up
- **Options pricing** — Black-Scholes IV surface on top of the existing price feed

---

## Questions I can answer about this

Built this to be able to talk through every layer in a technical interview. Some examples:

- *Why TimescaleDB over InfluxDB?* — SQL compatibility, Postgres ecosystem, better compression for our access pattern
- *Why historical simulation VaR instead of Monte Carlo VaR?* — We'd need to calibrate a covariance model; historical simulation is non-parametric and simpler for daily risk
- *What's the latency from price tick to dashboard update?* — Polygon → Kafka (~5ms) → consumer → Redis (~1ms) → WebSocket broadcaster → browser = under 200ms end-to-end
- *How does the SLSQP solver handle the long-only constraint?* — It's a box constraint `(0, 1)` on each weight variable, plus an equality constraint `Σwᵢ = 1`

---

## Stack

Python · FastAPI · Kafka · TimescaleDB · Redis · NumPy · SciPy · React · TypeScript · Vite · Recharts · Docker · Nginx · Prometheus · GitHub Actions
