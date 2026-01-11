from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


DATA_DIR = Path("/opt/airflow/data/raw")
SCRIPTS_DIR = Path("/opt/airflow/scripts")


def _pg_conn():
    return psycopg2.connect(
        host=os.environ["WAREHOUSE_HOST"],
        port=int(os.environ.get("WAREHOUSE_PORT", "5432")),
        dbname=os.environ["WAREHOUSE_DB"],
        user=os.environ["WAREHOUSE_USER"],
        password=os.environ["WAREHOUSE_PASSWORD"],
    )


RAW_DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.users (
  user_id            BIGINT PRIMARY KEY,
  signup_ts          TIMESTAMPTZ NOT NULL,
  country            TEXT NOT NULL,
  marketing_channel  TEXT NOT NULL,
  fav_sport          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.content (
  content_id     BIGINT PRIMARY KEY,
  content_title  TEXT NOT NULL,
  publish_ts     TIMESTAMPTZ NOT NULL,
  content_type   TEXT NOT NULL,
  author_id      BIGINT NOT NULL,
  content_url    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.sessions (
  session_id         TEXT PRIMARY KEY,
  user_id            BIGINT NOT NULL,
  session_start_ts   TIMESTAMPTZ NOT NULL,
  platform_type      TEXT NOT NULL,
  country            TEXT NOT NULL,
  state_province     TEXT NOT NULL,
  city               TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.user_content_events (
  event_id              BIGINT PRIMARY KEY,
  user_id               BIGINT NOT NULL,
  session_id            TEXT NOT NULL,
  content_id            BIGINT NOT NULL,
  event_ts              TIMESTAMPTZ NOT NULL,
  event_type            TEXT NOT NULL,
  time_spent_seconds    INT NOT NULL,
  scroll_count          INT NOT NULL,
  completion_pct        INT NOT NULL,
  completed_flag        SMALLINT NOT NULL,
  live_game_flag        SMALLINT NOT NULL,
  major_tournament_flag SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.categories (
  category_id        BIGINT PRIMARY KEY,
  category_name      TEXT NOT NULL,
  category_level     TEXT NOT NULL,
  parent_category_id BIGINT,
  category_desc      TEXT
);

CREATE TABLE IF NOT EXISTS raw.content_category (
  content_id          BIGINT NOT NULL,
  category_id         BIGINT NOT NULL,
  relationship_type   TEXT NOT NULL,
  source              TEXT NOT NULL,
  confidence_score    NUMERIC(5,3),
  attribution_weight  NUMERIC(6,3)
);
"""


def ensure_raw_tables():
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(RAW_DDL)
        conn.commit()


def generate_csvs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    cmd = ["python", str(SCRIPTS_DIR / "generate_data.py"), "--out", str(DATA_DIR)]
    subprocess.run(cmd, check=True)


def _copy_csv(conn, table: str, csv_path: Path, columns: list[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table};")
        copy_sql = f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv, HEADER true)"
        with csv_path.open("r", encoding="utf-8") as f:
            cur.copy_expert(copy_sql, f)


def load_raw():
    required = [
        "users.csv",
        "content.csv",
        "sessions.csv",
        "user_content_events.csv",
        "categories.csv",
        "content_category.csv",
    ]
    for name in required:
        p = DATA_DIR / name
        if not p.exists():
            raise FileNotFoundError(f"Missing {p}. Run generate step first.")

    with _pg_conn() as conn:
        _copy_csv(conn, "raw.users", DATA_DIR / "users.csv",
                  ["user_id", "signup_ts", "country", "marketing_channel", "fav_sport"])
        _copy_csv(conn, "raw.content", DATA_DIR / "content.csv",
                  ["content_id", "content_title", "publish_ts", "content_type", "author_id", "content_url"])
        _copy_csv(conn, "raw.sessions", DATA_DIR / "sessions.csv",
                  ["session_id", "user_id", "session_start_ts", "platform_type", "country", "state_province", "city"])
        _copy_csv(conn, "raw.user_content_events", DATA_DIR / "user_content_events.csv",
                  ["event_id", "user_id", "session_id", "content_id", "event_ts", "event_type",
                   "time_spent_seconds", "scroll_count", "completion_pct", "completed_flag",
                   "live_game_flag", "major_tournament_flag"])
        _copy_csv(conn, "raw.categories", DATA_DIR / "categories.csv",
                  ["category_id", "category_name", "category_level", "parent_category_id", "category_desc"])
        _copy_csv(conn, "raw.content_category", DATA_DIR / "content_category.csv",
                  ["content_id", "category_id", "relationship_type", "source", "confidence_score", "attribution_weight"])
        conn.commit()


default_args = {"owner": "noorsports", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="noorsports_raw_ingest",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["noorsports", "raw", "postgres", "local"],
) as dag:

    t1 = PythonOperator(task_id="ensure_raw_tables", python_callable=ensure_raw_tables)
    t2 = PythonOperator(task_id="generate_csvs", python_callable=generate_csvs)
    t3 = PythonOperator(task_id="load_raw_idempotent", python_callable=load_raw)

    t1 >> t2 >> t3

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt && dbt deps",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd /opt/airflow/dbt &&
        dbt run &&
        dbt run --full-refresh -s marts.user_content_interaction_fact
        """,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test",
    )

    t3 >> dbt_deps >> dbt_run >> dbt_test
