"""
Centralised configuration for all DAGs.

Sensitive values (email, connection IDs) are stored as Airflow Variables
and set via the Airflow UI — never hardcoded in code.

Variable lookups are wrapped in getter functions with lru_cache so they
are resolved at task runtime (not at DAG parse time) and each variable
is read from the metadata DB only once per worker process.

Table names and database names are plain constants (not sensitive, no DB call).
"""

from functools import lru_cache

from airflow.models import Variable


# ── Lazy Variable getters (resolved at task runtime, cached per process) ──────

@lru_cache(maxsize=1)
def get_clickhouse_conn_id() -> str:
    """ClickHouse connection ID from Airflow Variables."""
    return Variable.get("clickhouse_conn_id", default_var="clickhouse_default")


@lru_cache(maxsize=1)
def get_alert_email() -> str:
    """Failure notification email from Airflow Variables."""
    return Variable.get("alert_email", default_var="changeme@example.com")


@lru_cache(maxsize=1)
def get_s3_bucket() -> str:
    """S3 bucket for gold Parquet export from Airflow Variables."""
    return Variable.get("gold_export_s3_bucket", default_var="your-bucket")


@lru_cache(maxsize=1)
def get_s3_prefix() -> str:
    """S3 key prefix for gold export from Airflow Variables."""
    return Variable.get("gold_export_s3_prefix", default_var="data")


@lru_cache(maxsize=1)
def get_aurora_conn_id() -> str:
    """Aurora PostgreSQL connection ID from Airflow Variables."""
    return Variable.get("aurora_conn_id", default_var="aurora_medallion_etl")


# ── Databases (one per medallion layer) ───────────────────────────────────────
BRONZE_DATABASE = "bronze"
SILVER_DATABASE = "silver"
GOLD_DATABASE = "gold"
CATALOG_DATABASE = "catalog"

# ── Table names (follow naming conventions per layer) ─────────────────────────
# Bronze: stg_ prefix  (staging — raw, as-is from source)
BRONZE_STG_EVENTS = "stg_events"

# Silver: fact_ / dim_ prefix  (dimensional model — cleaned & conformed)
SILVER_FACT_EVENTS = "fact_events"
SILVER_QUARANTINE_EVENTS = "quarantine_events"
SILVER_DIM_USERS = "dim_users"

# Gold: agg_ / rpt_ prefix  (aggregates & reports — business-ready)
GOLD_AGG_EVENTS_HOURLY = "agg_events_hourly"
GOLD_AGG_HOURLY_HISTORY = "agg_events_hourly_history"
GOLD_RPT_DAILY_SUMMARY = "rpt_daily_summary"
GOLD_RPT_DAILY_HISTORY = "rpt_daily_summary_history"

# ── Catalog (metadata governance) ─────────────────────────────────────────────
CATALOG_TABLE_REGISTRY = "table_registry"
CATALOG_COLUMN_REGISTRY = "column_registry"
CATALOG_DATA_LINEAGE = "data_lineage"
CATALOG_PIPELINE_RUNS = "pipeline_runs"
CATALOG_VALIDATION_LOG = "validation_log"
CATALOG_BENCHMARK_LOG = "benchmark_log"

# ── Aurora schema (not sensitive — just a name) ──────────────────────────────
AURORA_SCHEMA = "medallion_etl"

# ── Event generator defaults ──────────────────────────────────────────────────
EVENT_CATEGORIES = ["page_view", "click", "purchase", "signup", "logout"]
EVENT_SOURCES = ["web", "mobile_ios", "mobile_android", "api", "partner"]
EVENT_ROWS_MIN = 5
EVENT_ROWS_MAX = 15
