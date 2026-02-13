"""
Data generation layer (Extract / Source).

Pure Python — no Airflow imports, no DB imports.
Easy to unit-test in isolation.
"""

import random
import string
from datetime import datetime, timedelta
from typing import Dict, List

from common.config import (
    EVENT_CATEGORIES,
    EVENT_ROWS_MAX,
    EVENT_ROWS_MIN,
    EVENT_SOURCES,
)


def _random_name(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=length))


def generate_events(
    execution_start: datetime,
    batch_id: str,
    min_rows: int = EVENT_ROWS_MIN,
    max_rows: int = EVENT_ROWS_MAX,
) -> List[Dict]:
    """
    Return a list of random event dicts for one execution window.

    Parameters
    ----------
    execution_start : datetime
        Start of the hourly window (Airflow data_interval_start).
    batch_id : str
        Unique identifier for this batch (Airflow run_id).
    min_rows / max_rows : int
        Range of rows to generate per batch.
    """
    num_rows = random.randint(min_rows, max_rows)
    rows: List[Dict] = []

    for i in range(num_rows):
        offset = timedelta(seconds=random.randint(0, 3599))
        rows.append(
            {
                "event_id": f"{batch_id}_{i}",
                "user_name": _random_name(),
                "category": random.choice(EVENT_CATEGORIES),
                "source": random.choice(EVENT_SOURCES),
                "value": round(random.uniform(0, 500), 2),
                "event_time": (execution_start + offset).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "batch_id": batch_id,
            }
        )

    return rows
