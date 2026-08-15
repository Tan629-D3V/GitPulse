# GitPulse

Realtime + batch analytics for predicting GitHub repository abandonment risk.

GitPulse is an early-warning system that continuously watches a curated set of GitHub
repositories, learns each repo's normal activity baseline from historical trends, and
flags repos showing genuine signs of decline before they go fully silent.

Contents
- Why GitPulse exists
- How it works (detailed)
- Results and metrics
- Tech stack and service responsibilities
- Quick start (local & Docker)
- Configuration examples
- Model details (features, labels, training, evaluation)
- Project structure (file-level)
- Troubleshooting & tips
- Development & contributing
- License & contact

---

Why GitPulse exists
-------------------
Millions of GitHub repositories quietly go inactive every year — no commits, no releases,
no maintainer response — often with zero warning to the people and companies relying on them.
By the time abandonment is obvious, downstream systems and users may already be exposed to
unpatched vulnerabilities, broken builds, and unanswered issues.

GitPulse is designed to provide an early signal: combining historical baseline behavior with
live event streams so we can detect abnormal decline (low momentum) and surface that risk
to maintainers and downstream teams.

Key promise: catch meaningful declines early (favoring recall) while keeping false alarms
reasonable so alerts are actionable.

---

How it works (detailed)
-----------------------
There are two independent data pipelines feeding the same feature DB and model:

1) Streaming (Realtime)
   - Source: GitHub Events API polled per repo (producer)
   - Buffer: Apache Kafka topic(s)
   - Consumer: Kafka consumer parses events and writes raw events to Postgres (`github_events_raw`)
   - Use: live inference input, near-real-time indicators (last 24–72 hours)

2) Batch (Historical baseline)
   - Source: GH Archive (hourly event dumps; we keep ~60 days for model training)
   - Processing: Apache Spark ETL job produces weekly aggregated features
   - Storage: MinIO (bronze/landing), Postgres (weekly features table `repo_weekly_features`)
   - Use: establishes what "normal" looks like per repo (rolling averages, trends)

Overall flow (ASCII):

 GitHub Events API  ──▶  Kafka  ──▶  PostgreSQL (github_events_raw)  ─┐
   (live per-repo)       (buffer)        (raw events)              │
                                                           ├──▶  XGBoost  ──▶  Streamlit Dashboard (Live view)
 GH Archive (60d)   ──▶  Spark  ──▶  PostgreSQL (repo_weekly_features) ┘
   (historical)          (ETL)           (weekly features)

Why two pipelines?
- Batch gives long-term baseline and rolling statistics for each repo.
- Streaming gives the current signal. Comparing current momentum to baseline is the core of “abandonment risk”.

Data schema highlights
- github_events_raw: raw event JSON + parsed columns (repo_id, event_type, actor, ts)
- repo_weekly_features: repo_id, week_start, push_count, star_count, fork_count, issue_count, pr_count, comment_count, push_4wk_avg, star_4wk_avg, activity_score, momentum_delta, etc.

---

Results (short summary)
-----------------------
We prioritized recall (catching true abandonments) over precision because missing a true abandoned repo has higher cost.

Metrics (XGBoost)
- 5-Fold CV mean / Held-out test:
  - Accuracy: 0.830 / 0.854
  - Precision: 0.587 / 0.577
  - Recall: 0.647 / 0.882
  - F1-score: 0.604 / 0.698

Live numbers
- 60,937+ real streamed events
- 1,081 weekly feature rows
- 263 repos with full batch history
- 455 curated repos across 5 categories

Interpretation: Test recall 0.882 indicates few missed abandonments; expect more false positives, which is acceptable for an alerting/awareness product.

---

Tech stack (responsibilities)
-----------------------------
- Streaming: Apache Kafka (KRaft mode) — message durability and decoupling
- Producer/Consumer: Python (async) — polling GitHub, publishing/consuming events
- Batch ETL: Apache Spark — transforms GH Archive into weekly features
- Object store: MinIO — stores raw/bronze artifacts for reproducibility
- Database: PostgreSQL — canonical storage for raw events and weekly features
- ML: XGBoost (training & inference) + scikit-learn utilities (cross-validation)
- Dashboard: Streamlit — live + historical visualizations and repo score explorer
- Orchestration: Docker Compose (11 services) for local reproducible setup; Airflow provisioned for future scheduling

All services are containerized and can be launched via docker-compose for an integrated local environment.

---

Quick start (local, recommended Docker)
--------------------------------------
Prereqs:
- Docker 20.10+, Docker Compose v2
- (Optional) Python 3.8+ for local CLI & development

1) Clone repository
```bash
git clone https://github.com/Tan629-D3V/GitPulse.git
cd GitPulse
