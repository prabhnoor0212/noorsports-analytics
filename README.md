# NoorSports Analytics  
**Local Airflow + dbt + Postgres Analytics Engineering Project**

This repository contains a fully local, end-to-end analytics engineering project for **NoorSports**, a hypothetical sports/media platform. The goal is to demonstrate production-grade thinking around **orchestration, data modeling, transformations, testing, and reproducibility**, without relying on any cloud services.

The project runs entirely on **Docker Compose** using:
- **Airflow 2.x (LocalExecutor)** for orchestration
- **Postgres** as the analytics warehouse
- **dbt (dbt-postgres)** for transformations, tests, and documentation

---

## 1. Project Theme

NoorSports is a sports and media platform where:
- Users browse sports content (news, videos, highlights, game events)
- Sessions generate engagement events such as scrolls, watches, and completions
- Content can reference multiple sports entities (teams, leagues, sports)

The warehouse is designed to answer questions like:
- Which sports or leagues drive the most engagement?
- How does engagement differ by platform or geography?
- Which content types perform best?
- How many **user × session × content** interactions occur daily?

Synthetic data is generated locally to simulate realistic behavior.

---

## 2. Architecture Overview

### High-level flow

```text
+-----------------------------+
| Airflow (LocalExecutor)     |
| DAG: noorsports_raw_ingest  |
+--------------+--------------+
               |
               v
+-----------------------------+
| Generate synthetic CSVs     |
| ./data/raw/*.csv            |
+--------------+--------------+
               |
               v
+-----------------------------+
| Load raw tables in Postgres |
| raw.*  (TRUNCATE + COPY)    |
+--------------+--------------+
               |
               v
+-----------------------------+
| dbt run (staging + marts)   |
| dbt test                    |
+--------------+--------------+
               |
               v
+-----------------------------+
| marts.* dimensional models  |
+-----------------------------+
```


### Key design principles
- **Fully local**: no cloud dependencies
- **Idempotent**: DAG can be rerun safely
- **Test-driven**: dbt tests gate correctness
- **Analytics-first modeling**: star schema with clear grain

---

## 3. Orchestration (Airflow)

### DAG: `noorsports_raw_ingest`

The DAG represents a realistic batch analytics pipeline:

1. **ensure_raw_tables**
   - Creates the `raw` schema and raw tables if they don’t exist

2. **generate_csvs**
   - Runs a Python script to generate synthetic CSVs
   - Output written to `./data/raw`

3. **load_raw_idempotent**
   - Loads CSVs into Postgres using `TRUNCATE + COPY`
   - Ensures reruns never create duplicates

4. **dbt_deps**
   - Standard dbt step (included for realism)

5. **dbt_run**
   - Builds all staging and mart models
   - Performs a targeted `--full-refresh` of the incremental fact
     (explained later)

6. **dbt_test**
   - Runs all dbt tests
   - Fails the DAG if any data quality checks fail

### Why `TRUNCATE + COPY` for raw?
- Simple and deterministic for local pipelines
- Ensures raw tables are always a clean snapshot
- Mimics early-stage batch ingestion patterns

---

## 4. dbt Project Structure

The dbt project follows a standard layered approach:

### Sources
Defined in `sources.yml`, pointing to `raw.*` tables.

### Staging models (`staging.*`)
Responsibilities:
- Type casting (timestamps, integers, flags)
- Normalized column naming
- Derived fields (e.g., `event_date`, `session_date`)
- First layer of data quality tests

Staging models are materialized as **views** to encourage iteration.

---

## 5. Dimensional Model (Marts)

The marts layer is a star-schema-oriented model designed for analytics and BI.

---

### 5.1 Fact Table

#### `marts.user_content_interaction_fact`

**Grain:**  
**user × session × content**

This represents the fundamental behavioral unit:
> “A user interacted with a specific content item within a session.”

**Primary key:**  
`content_session_id`  
(deterministic hash of `user_id | session_id | content_id`)

**Measures:**
- `time_spent_seconds` (sum)
- `scroll_count` (sum)
- `completion_pct_max` (max)
- `completed_flag` (max)
- `live_game_flag`
- `major_tournament_flag`

**Timestamps:**
- `first_event_ts`
- `last_event_ts`
- `max_event_ts` (used for incremental logic)

**Why this grain?**
- Avoids overly granular event-level facts
- Supports session analysis and content performance
- Enables downstream marts (daily user activity, cohorts, retention)

---

### 5.2 Incremental Strategy (Important)

The fact table is **incremental with merge**:
- `materialized = incremental`
- `unique_key = content_session_id`
- `incremental_strategy = merge`

However, raw ingestion uses **TRUNCATE + COPY**, which means:
- Raw data is replaced each run
- Session IDs and events change between runs

**Design decision:**
- Keep the fact model incremental (realistic pattern)
- After raw replacement, force a **full refresh of the fact only**

This is handled in Airflow by running:
- `dbt run`
- then `dbt run --full-refresh -s marts.user_content_interaction_fact`

This prevents stale incremental rows referencing old sessions.

---

## 6. Dimensions

### `marts.user_dim` (SCD2 scaffold)

Includes:
- `user_sk` (surrogate key)
- `user_id` (natural key)
- `row_active_start_ts`
- `row_active_end_ts`
- `is_current`

**Why SCD2?**
User attributes (preferences, cohorts, geo, acquisition channel) change over time.  
SCD2 preserves historical truth for time-based analysis.

**Note:**  
The synthetic dataset currently produces one version per user, so this is an SCD2 **scaffold**. The structure is in place for future change simulation.

---

### `marts.content_dim`
- Metadata about content items
- Current-state (SCD1-style)

### `marts.content_type_dim`
- Lookup dimension (`news`, `video`, `game_event`, etc.)
- Separated for clarity and filtering

### `marts.platform_dim`
- Platform lookup (`iOS`, `Android`, `Web`)

### `marts.geo_dim`
- Country / state / city
- Derived from sessions
- Surrogate key generated via deterministic hash

### `marts.date_dim`
- Generated calendar table
- Supports day/week/month rollups
- Derived from observed event/session date range

---

## 7. Sports Hierarchy & Tagging

### Unified Sports Entity Dimension

#### `marts.category_dim`
Represents **sport / league / team** in a single table:
- `category_level`: `sport`, `league`, `team`
- `parent_category_id` enables hierarchy

This avoids separate tables for sport/league/team and allows flexible depth.

---

### Closure Table

#### `marts.category_closure_bridge`
Precomputed ancestor → descendant relationships:
- Enables rollups like “team → league → sport”
- Avoids recursive queries in BI tools
- Built via recursive CTE in dbt

---

### Content Tagging (Many-to-Many)

#### `marts.content_category_bridge`
Maps content → sports entities:
- Content can reference multiple teams/leagues/sports
- Supports NLP/editorial tagging use-cases
- Includes relationship type and attribution fields

---

## 8. Data Quality & Testing

The project includes:
- `not_null` and `unique` tests on all keys
- Relationship tests enforcing referential integrity:
  - Fact → dimensions
  - Events → sessions/users/content
- Tests are executed in Airflow and **fail the DAG** if violated

This mirrors production analytics engineering standards.

---

## 9. Why This Design Works Well for Analytics Engineering

This project demonstrates:
- **End-to-end ownership**: ingestion → modeling → validation
- **Operational thinking**: idempotency, incremental trade-offs, consistency
- **Strong modeling fundamentals**: grain clarity, star schema, bridges
- **Tooling fluency**: Airflow + dbt + Postgres
- **Data quality mindset**: tests as first-class citizens

---

## 10. Running the Project Locally

### Prerequisites
- Docker Desktop (running)
- No cloud accounts required

### Steps
```bash
cp .env.example .env
docker compose up -d --build
```
Open Airflow UI:

http://localhost:8080

Login: admin / admin

Trigger DAG: noorsports_raw_ingest

Verify marts:
```bash
docker compose exec warehouse psql -U warehouse -d noorsports -c "\dt marts.*"
```

## 11. Possible Extensions

- Simulate real SCD2 changes in user_dim
- Add daily or cohort-based marts
- Introduce late-arriving events
- Add BI dashboards or semantic layer