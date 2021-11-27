from datetime import date, datetime
from typing import Optional

from google.cloud import bigquery

from bqdataprofiler.models.setting import ProjectSetting


def column_name_to_metric_target(col_name: str) -> tuple[str, Optional[str]]:
    """
    Return (metric_name, column_name) from column name that build by this class.
    """
    parts = col_name.split("__")
    if parts[0] == "table_metric":
        return parts[1], None
    if parts[0] == "column_metric":
        return parts[1], parts[2]
    raise ValueError(f"col_name: {col_name} may not be built by this class")


def python_type_to_bigquery_type(tp: type) -> str:
    """
    Convert python type to bigquery type name
    """
    if tp is int:
        return "INT64"
    if tp is float:
        return "FLOAT64"
    if tp is date:
        return "DATE"
    if tp is datetime:
        return "TIMESTAMP"
    return str(tp)


class BigQueryProfileQueryBuilder:
    """
    Profiling query builder for BigQuery
    """
    _NUMERIC_TYPES = {"INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"}
    _TIME_TYPES = {"TIMESTAMP", "DATETIME", "DATE"}
    _ORDERABLE_TYPES = _NUMERIC_TYPES | _TIME_TYPES
    _COMPOUND_TYPES = {"RECORD", "STRUCT"}
    _ZERO_OR_EMPTY_TYPES = _NUMERIC_TYPES | {"STRING", "BYTES"}

    def __init__(self, setting: ProjectSetting):
        self.setting = setting

    def build_query_for_table_profile(self, table: bigquery.Table) -> str:
        """
        Build query string for table profiling
        """
        # table row count metric
        column_exprs: list[tuple[str, str]] = [("table_metric__row_count", self.get_row_count())]

        # aggregate column metrics
        col: bigquery.SchemaField
        for col in table.schema:
            if self.is_compound_type(col):
                continue
            if self.setting.include_null_count:
                column_exprs.append((f"column_metric__null__{col.name}", self.get_null_count(col.name)))
                column_exprs.append((f"column_metric__null_rate__{col.name}", self.get_null_rate(col.name)))
            if self.setting.include_empty_or_zero_count and self.is_zero_or_empty_type(col):
                column_exprs.append(
                    (f"column_metric__empty_or_zero__{col.name}", self.get_zero_or_empty(col.name, col.field_type)))
            if self.is_orderable(col):
                if self.setting.include_max_value:
                    column_exprs.append((f"column_metric__max__{col.name}", self.get_max(col.name)))
                if self.setting.include_min_value:
                    column_exprs.append((f"column_metric__min__{col.name}", self.get_min(col.name)))

        # build query
        return f"""SELECT
 {",".join([f"{e} AS {n}" for n, e in column_exprs])}
 FROM `{table.project}.{table.dataset_id}.{table.table_id}`"""

    def is_orderable(self, col: bigquery.SchemaField) -> bool:
        """
        Return if orderable type
        """
        return col.field_type.upper() in self._ORDERABLE_TYPES

    def is_compound_type(self, col: bigquery.SchemaField) -> bool:
        """
        Return if compound type
        """
        if col.field_type.upper() in self._COMPOUND_TYPES or col.mode.upper() == "REPEATED":
            return True
        return False

    def is_zero_or_empty_type(self, col: bigquery.SchemaField) -> bool:
        """
        Return if zero or empty supported type
        """
        return col.field_type in self._ZERO_OR_EMPTY_TYPES

    def get_row_count(self) -> str:
        """
        Return expression for row count
        """
        return "COUNT(*)"

    def get_max(self, col_name: str) -> str:
        """
        Return expression for max
        """
        return f"MAX({col_name})"

    def get_min(self, col_name: str) -> str:
        """
        Return expression for min
        """
        return f"MIN({col_name})"

    def get_null_count(self, col_name: str) -> str:
        """
        Return expression for null count
        """
        return f"COUNTIF({col_name} IS NULL)"

    def get_null_rate(self, col_name: str) -> str:
        """
        Return expression for null rate
        """
        return f"SAFE_DIVIDE(COUNTIF({col_name} IS NULL), {self.get_row_count()})"

    def get_zero_or_empty(self, col_name: str, col_type: str):
        """
        Return expression for zero or empty
        """
        if col_type in self._NUMERIC_TYPES:
            return f"COUNTIF({col_name} = 0)"
        if col_type == "STRING":
            return f'COUNTIF({col_name} = "")'
        if col_type == "BYTES":
            return f'COUNTIF({col_name} = CAST("" AS BYTES))'
        raise ValueError(f"Cannot get zero_or_empty value. col_name: {col_name} col_type: {col_type}")
