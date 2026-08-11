# GitPulse

**Real-Time & Batch Analytics for Predicting GitHub Repository Abandonment Risk**

A data engineering + machine learning diploma project combining streaming and batch pipelines
to monitor a curated set of GitHub repositories and predict abandonment risk before a project
goes fully silent.

Project by: Tanmay Chouhan (262549), Atharva Waghmare (262505) — PGCP-BDA, CDAC

---

## Problem Statement

Open-source ecosystems are fragile — millions of repositories quietly go inactive every year,
often without warning to the maintainers, contributors, or downstream users who depend on
them. There's no lightweight, self-hosted way to continuously watch a curated set of
repositories and flag declining momentum before a project is effectively dead. GitPulse builds
that early-warning system, and in doing so demonstrates a full modern data engineering stack:
combining real-time event streams with historical batch trends.

**Who this is for**: engineering leads evaluating which open-source dependency to adopt
long-term, maintainers/investors tracking project health across a portfolio, and platform
teams auditing supply-chain risk across many dependencies.

---

## Status: Core Pipeline + Model Complete

| Component | Status |
|---|---|
| Streaming pipeline (GitHub Events API → Kafka → Postgres) | Complete — 60,000+ live events captured |
| Batch pipeline (GHArchive → Spark → MinIO → Postgres) | Complete — 1,081 weekly feature rows across 263 repos |
| Weekly feature engineering (momentum, rolling averages) | Complete |
| XGBoost abandonment-risk model — trained + validated | Complete |
| Live inference against streaming data | Complete (`models/src/infer.py` runs standalone) |
| Full containerized Docker Compose stack | Complete — 11 services |
| Dashboard (batch + live views) | Complete |
| Dashboard "Live Predictions" section | Not wired yet — see below |
| Airflow DAG scheduling | Not built — container runs, nothing scheduled |
| Cloud deployment | Not started |

---

## Architecture

```
Batch:      GHArchive (60 days, ~1.2GB) -> Spark -> MinIO (bronze) -> Postgres (repo_weekly_features)
Streaming:  GitHub REST API (per-repo poll, 455 repos) -> Kafka producer -> Kafka topic
            -> Kafka consumer -> Postgres (github_events_raw)
Model:      repo_weekly_features (training) + github_events_raw (live inference input)
            -> XGBoost -> abandonment_risk_score per repo
```

Both pipelines write into the same PostgreSQL database. The model container trains on the
batch (historical) feature table and runs inference against recent streaming data, following
a standard train-on-batch / infer-on-live pattern.

**Why two pipelines**: predicting "is this repo declining" requires both a historical baseline
(what's normal for this repo) and a current signal (what's happening now) — neither alone is
sufficient. Batch establishes the baseline; streaming provides the live comparison point.

---

## Dataset

- **455 curated repositories** across 5 categories (general popularity, Python, machine
  learning, big-data, data-engineering), sourced via GitHub's Search API
- **60,937+ live streamed events** in `github_events_raw`
- **1,081 weekly feature rows** in `repo_weekly_features`, covering **263 distinct repos**
  with batch history (average ~4.1 weeks of history per repo)
- **Sources**: GitHub Events API (live, per-repo polling) + GH Archive (60 days of hourly
  historical snapshots, ~1.2GB downloaded)
- **Feature columns**: `push_count`, `star_count`, `fork_count`, `issue_count`, `pr_count`,
  `create_count`, `comment_count`, `push_count_4wk_avg`, `star_count_4wk_avg`,
  `activity_score`, `momentum_delta`

---

## Machine Learning

**Label**: a repo-week is labeled abandonment risk = 1 if `activity_score` stays at/near zero
for the following 3 consecutive weeks — a forward-looking label derived directly from
`repo_weekly_features`.

**Validation**: 80/20 stratified train/test split, plus 5-fold Stratified K-Fold cross-
validation on the training set (chosen because the abandonment class is a minority class).

**Class imbalance**: handled via XGBoost's `scale_pos_weight`, set to the negative/positive
class ratio per training fold, rather than naive resampling.

**Results (XGBoost)**:

| Metric | 5-fold CV mean | Held-out test |
|---|---|---|
| Accuracy | 0.830 | 0.854 |
| Precision | 0.587 | 0.577 |
| Recall | 0.647 | 0.882 |
| F1-score | 0.604 | 0.698 |

High recall (0.88 on test) was prioritized by design — for an early-warning system, missing a
genuinely abandoned repo is costlier than a false alarm.

---

## Stack

| Layer | Technology | Why |
|---|---|---|
| Streaming broker | Apache Kafka (KRaft mode) | Durable, ordered, decouples producer/consumer; no Zookeeper dependency |
| Ingestion (real-time) | Python producer/consumer | Simple, fully containerized |
| Ingestion (historical) | GH Archive | Free, hourly public event dumps |
| Batch processing | Apache Spark | Distributed ETL, native window functions for rolling features |
| Object storage | MinIO (S3-compatible) | Local drop-in for Spark's `hadoop-aws` connector |
| Database | PostgreSQL | Relational, well-suited to the joined feature/event schema |
| ML | XGBoost | Strong on small/medium tabular data, handles imbalance natively |
| Orchestration | Docker Compose (11 services) | Reproducible, one-command startup |
| Dashboard | Streamlit | Fast to build, demo-ready |
| Scheduling (planned) | Apache Airflow | Container provisioned, DAG not yet built |

---

## Project Structure

```
.
├── dashboard/            # Streamlit dashboard (Docker service)
├── dags/                 # Airflow DAGs (not yet written)
├── data/                 # GHArchive samples, MinIO/Postgres/Kafka data volumes
├── docker/               # docker-compose.yml, .env, service configs
├── models/
│   └── src/
│       ├── features.py   # Shared feature builder (train + infer use the same function)
│       ├── train.py      # Trains XGBoost, saves artifacts/model.pkl
│       └── infer.py      # Loads model, scores recent streaming activity per repo
├── spark_jobs/
│   └── spark_job.py      # Batch transform: GHArchive -> MinIO bronze -> Postgres gold
├── stream/
│   ├── producer.py       # Polls GitHub Events API per curated repo, publishes to Kafka
│   └── consumer.py       # Reads Kafka, writes github_events_raw
├── curated_repos.txt     # 455 repos across 5 categories
└── docs/                 # Full documentation set (see below)
```

---

## Quick Start

```bash
cd docker
docker compose up -d
docker compose ps
```
Open the dashboard: `http://localhost:8501`

Run inference standalone:
```bash
docker compose exec -T model python3 src/infer.py
```

See `docs/QUICK_RUN.md` for the short version, or `docs/MY_START_GUIDE.md` for full detail
including troubleshooting.

---

## Documentation

| File | Contents |
|---|---|
| `docs/QUICK_RUN.md` | Minimal steps to get running |
| `docs/MY_START_GUIDE.md` | Full startup guide with troubleshooting index |
| `docs/PROBLEMS_AND_SOLUTIONS.md` | Every real issue hit during development, root cause + fix |
| `docs/CODE_REFERENCE.md` | Function-by-function explanation of the streaming/batch code |
| `docs/PPT_EXPLANATION_AND_QA.md` | Slide-by-slide presentation walkthrough with anticipated Q&A |

---
## License

Diploma project — not licensed for external use.
