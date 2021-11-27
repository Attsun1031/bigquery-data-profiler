import re
from unittest.mock import MagicMock, patch

from google.cloud import bigquery

import bqdataprofiler.profiler as sut
from bqdataprofiler.models.setting import ProjectSetting
from bqdataprofiler.models.result import Metric
from bqdataprofiler.querybuilder import BigQueryProfileQueryBuilder


class TestCollectTables:
    """
    Tests for _collect_tables
    """
    ds_and_tables = {
        "ds1": ["tbl1", "tbl2"],
        "ds2": ["tbl3", "tbl4"],
    }

    def _build_bq_mock(self):
        m_bq = MagicMock(bigquery.Client)
        m_bq.list_datasets.return_value = [
            MagicMock(bigquery.dataset.DatasetListItem, dataset_id=ds)
            for ds in self.ds_and_tables.keys()
        ]
        ds_to_tbls = {
            ds: [
                MagicMock(bigquery.table.TableListItem, table_id=tbl,
                          reference=MagicMock(bigquery.TableReference, table_id=tbl))
                for tbl in tbls]
            for ds, tbls in self.ds_and_tables.items()
        }

        m_bq.list_tables.side_effect = lambda ds: ds_to_tbls[ds.dataset_id]
        m_bq.get_table.side_effect = lambda tbl: MagicMock(bigquery.Table, table_id=tbl.table_id)
        return m_bq

    def test_no_pattern_setting(self):
        """
        When no pattern settings, then return everything.
        """
        setting = ProjectSetting(project_name="test-project")
        m_bq = self._build_bq_mock()
        tables = sut._collect_tables(m_bq, setting)
        assert set([t.table_id for t in tables]) == {"tbl1", "tbl2", "tbl3", "tbl4"}

    def test_with_patterns(self):
        """
        When dataset and table pattern settings, then return matched.
        """
        setting = ProjectSetting(
            project_name="test-project",
            include_schema_patterns=[re.compile(r"ds1"), re.compile(r"ds3")],
            include_table_patterns=[re.compile(r"bl1"), re.compile(r"bl3")]
        )
        m_bq = self._build_bq_mock()
        tables = sut._collect_tables(m_bq, setting)
        assert set([t.table_id for t in tables]) == {"tbl1"}


class TestProfileTable:
    """
    Tests for _profile_table
    """

    def test_get_metrics(self):
        """
        When query succeeded, then return metrics
        """
        full_name = "test-project.ds1.tbl1"
        project_id, dataset_id, table_id = full_name.split(".")
        m_tbl = MagicMock(bigquery.Table, project=project_id, dataset_id=dataset_id, table_id=table_id)

        expected_total_bytes_processed = 1024
        m_job = MagicMock(bigquery.QueryJob, total_bytes_processed=expected_total_bytes_processed)
        expected_items = {
            "table_metric__row_count": 100,
            "column_metric__null__name": 10,
            "column_metric__null_rate__name": 0.1,
        }
        expected_rows = [expected_items]

        with patch("bqdataprofiler.profiler._run_query_job") as run_query_job:
            run_query_job.return_value = m_job, iter(expected_rows)
            metrics, total_bytes_processed = sut._profile_table(m_tbl, MagicMock(BigQueryProfileQueryBuilder), None,
                                                                False)

        assert total_bytes_processed == expected_total_bytes_processed
        assert metrics == [
            Metric(
                full_name=full_name, project_name=project_id, dataset_name=dataset_id, table_name=table_id,
                metric_name="row_count", value="100", value_type="INT64"
            ),
            Metric(
                full_name=full_name, project_name=project_id, dataset_name=dataset_id, table_name=table_id,
                metric_name="null", column_name="name", value="10", value_type="INT64"
            ),
            Metric(
                full_name=full_name, project_name=project_id, dataset_name=dataset_id, table_name=table_id,
                metric_name="null_rate", column_name="name", value="0.1", value_type="FLOAT64"
            ),
        ]
