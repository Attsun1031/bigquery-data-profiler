from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from bqdataprofiler.models.error import ErrorCollection


class Metric(BaseModel):
    """
    Result for each metric
    """
    full_name: str
    project_name: str
    dataset_name: str
    table_name: str
    column_name: Optional[str] = None
    metric_name: str
    value: str
    value_type: str


class ProfilingResult(BaseModel):
    """
    Results for each run
    """
    start_datetime: datetime
    end_datetime: Optional[datetime] = None
    duration_sec: int = 0
    run_id: str
    metrics: list[Metric] = []
    errors: Optional[ErrorCollection] = None
    total_bytes_processed: int = 0
