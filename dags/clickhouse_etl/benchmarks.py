"""
Benchmarking instrumentation for cross-framework performance comparison.

Captures per-task timing, throughput, and memory metrics and stores them
in catalog.benchmark_log.  Framework-agnostic: the same metrics can be
captured from Airflow or any other orchestrator by calling
record_benchmark() with the appropriate framework name.

Usage in Airflow tasks.py:
    with TaskBenchmark(context, "load_bronze", "bronze") as bm:
        # ... do work ...
        bm.rows_out = inserted_count
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import date as _date, datetime, timezone
from typing import Any, Dict, Optional

from clickhouse_etl.db import get_hook
from common.config import CATALOG_DATABASE, CATALOG_BENCHMARK_LOG

logger = logging.getLogger(__name__)

FRAMEWORK = "airflow"

try:
    import airflow
    FRAMEWORK_VERSION = airflow.__version__
except Exception:
    FRAMEWORK_VERSION = "unknown"


def _peak_memory_mb() -> float:
    """Return current process peak RSS in MB (cross-platform)."""
    try:
        import resource
        # Unix: ru_maxrss is in KB on Linux, bytes on macOS
        rusage = resource.getrusage(resource.RUSAGE_SELF)
        peak_kb = rusage.ru_maxrss
        import platform
        if platform.system() == "Darwin":
            return peak_kb / (1024 * 1024)  # bytes → MB
        return peak_kb / 1024  # KB → MB
    except ImportError:
        pass
    try:
        import psutil
        return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
    except ImportError:
        return 0.0


@dataclass
class TaskBenchmark:
    """Context manager that times a task and records the benchmark.

    Usage:
        with TaskBenchmark(context, "load_bronze", "bronze") as bm:
            rows = do_work()
            bm.rows_out = rows
    """

    context: Dict[str, Any]
    task_name: str
    layer: str
    rows_in: int = 0
    rows_out: int = 0
    extra: Dict[str, Any] = field(default_factory=dict)

    # Set automatically
    _start_time: float = field(default=0.0, init=False)
    _start_dt: Optional[datetime] = field(default=None, init=False)
    _status: str = field(default="success", init=False)

    def __enter__(self) -> "TaskBenchmark":
        self._start_time = time.perf_counter()
        self._start_dt = datetime.now(timezone.utc).replace(tzinfo=None)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        end_time = time.perf_counter()
        end_dt = datetime.now(timezone.utc).replace(tzinfo=None)
        duration_ms = int((end_time - self._start_time) * 1000)
        duration_s = duration_ms / 1000.0

        if exc_type is not None:
            self._status = "failed"

        rows_per_second = (
            self.rows_out / duration_s if duration_s > 0 else 0.0
        )

        try:
            event_date_str = self.context["data_interval_start"].strftime("%Y-%m-%d")
            event_date = _date.fromisoformat(event_date_str)
            event_hour = self.context["data_interval_start"].hour
            run_id = self.context.get("run_id", "unknown")
            batch_id = run_id
        except Exception:
            event_date = _date.today()
            event_hour = 0
            run_id = "unknown"
            batch_id = "unknown"

        record_benchmark(
            run_id=run_id,
            task_name=self.task_name,
            layer=self.layer,
            status=self._status,
            started_at=self._start_dt,
            ended_at=end_dt,
            duration_ms=duration_ms,
            rows_in=self.rows_in,
            rows_out=self.rows_out,
            rows_per_second=rows_per_second,
            peak_memory_mb=_peak_memory_mb(),
            event_date=event_date,
            event_hour=event_hour,
            batch_id=batch_id,
            extra=self.extra,
        )

        logger.info(
            "Benchmark: %s [%s] %dms | %d rows out | %.1f rows/s | %.1fMB peak",
            self.task_name, self._status, duration_ms,
            self.rows_out, rows_per_second, _peak_memory_mb(),
        )

        return False  # don't suppress exceptions


def record_benchmark(
    run_id: str,
    task_name: str,
    layer: str,
    status: str,
    started_at: datetime,
    ended_at: datetime,
    duration_ms: int,
    rows_in: int = 0,
    rows_out: int = 0,
    rows_per_second: float = 0.0,
    peak_memory_mb: float = 0.0,
    event_date: Optional[_date] = None,
    event_hour: int = 0,
    batch_id: str = "",
    extra: Optional[Dict[str, Any]] = None,
    framework: str = FRAMEWORK,
    framework_version: str = FRAMEWORK_VERSION,
) -> None:
    """Insert a single benchmark record into catalog.benchmark_log.

    This function is framework-agnostic: any orchestrator
    can call it directly with its own framework name.
    """
    extra_json = json.dumps(extra) if extra else "{}"

    try:
        get_hook(CATALOG_DATABASE).execute(
            f"INSERT INTO {CATALOG_BENCHMARK_LOG} "
            f"(run_id, framework, framework_version, task_name, layer, status, "
            f"started_at, ended_at, duration_ms, "
            f"rows_in, rows_out, rows_per_second, peak_memory_mb, "
            f"event_date, event_hour, batch_id, extra) VALUES",
            [(
                run_id, framework, framework_version, task_name, layer, status,
                started_at, ended_at, duration_ms,
                rows_in, rows_out, rows_per_second, peak_memory_mb,
                event_date or _date.today(), event_hour, batch_id, extra_json,
            )],
        )
    except Exception:
        logger.exception("Benchmark: failed to record metrics for %s", task_name)
