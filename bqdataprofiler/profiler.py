import datetime
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional

from google.cloud import bigquery
from loguru import logger

from bqdataprofiler.models.error import Error
from bqdataprofiler.models.result import Metric, ProfilingResult
from bqdataprofiler.models.setting import ProfileSettingRoot, ProjectSetting
from bqdataprofiler.querybuilder import (BigQueryProfileQueryBuilder, column_name_to_metric_target,
                                         python_type_to_bigquery_type)


def run_profile(root_setting: ProfileSettingRoot, result: ProfilingResult, dry_run: bool = False):
    """
    Run profiling
    """
    for project_setting in root_setting.settings:
        project_name = project_setting.project_name
        logger.info(f"Start profiling for project: {project_name}")

        bq_cli = bigquery.Client(project=project_name)
        tables = _collect_tables(bq_cli, project_setting)
        query_builder = BigQueryProfileQueryBuilder(project_setting)

        with ThreadPoolExecutor(max_workers=root_setting.max_workers) as executor:
            _execute_profiling(executor, dry_run, query_builder, result, root_setting, tables)
        logger.info(f"End profiling for project: {project_name}")

    result.end_datetime = datetime.datetime.now()
    result.duration_sec = (result.end_datetime - result.start_datetime).seconds


def _execute_profiling(executor, dry_run, query_builder, result, root_setting, tables):
    future_to_table = {}
    for tbl in tables:
        generated_future = executor.submit(_profile_table, tbl, query_builder, root_setting.maximum_byte_billed,
                                           dry_run)
        future_to_table[generated_future] = tbl
    for f in as_completed(future_to_table):
        tbl = future_to_table[f]
        try:
            table_metrics, total_bytes_processed = f.result()
            result.metrics.extend(table_metrics)
            result.total_bytes_processed += total_bytes_processed
        except Exception as e:
            err = Error(message=str(e), values={"full_table_id": tbl.full_table_id}, stacktrace=traceback.format_exc())
            result.errors.append(err)


def _collect_tables(bq_cli: bigquery.Client, project_setting: ProjectSetting) -> list[bigquery.Table]:
    tables: list[bigquery.Table] = []
    ds: bigquery.dataset.DatasetListItem
    tbl_list_item: bigquery.table.TableListItem
    for ds in bq_cli.list_datasets():
        if project_setting.include_schema_patterns:
            # filter dataset
            include_dataset = False
            for pat in project_setting.include_schema_patterns:
                if pat.search(ds.dataset_id):
                    include_dataset = True
                    break
            if not include_dataset:
                continue

        for tbl_list_item in bq_cli.list_tables(ds):
            name = f"{ds.dataset_id}.{tbl_list_item.table_id}"
            if project_setting.include_table_patterns:
                # filter table
                include_table = False
                for pat in project_setting.include_table_patterns:
                    if pat.search(name):
                        include_table = True
                        break
                if not include_table:
                    continue
            tbl = bq_cli.get_table(tbl_list_item.reference)
            tables.append(tbl)
    return tables


def _profile_table(tbl: bigquery.Table, query_builder: BigQueryProfileQueryBuilder, maximum_bytes_billed: Optional[int],
                   dry_run: bool) -> tuple[list[Metric], int]:  # list[metrics], total bytes processed
    logger.info(f"Profiling for table: {tbl.full_table_id}")
    job, result = _run_query_job(dry_run, maximum_bytes_billed, query_builder, tbl)
    total_bytes_processed: int = job.total_bytes_processed  # pylint: disable=no-member
    logger.info(
        f"Total bytes processed for table({tbl.full_table_id}): {total_bytes_processed / 1024 / 1024 / 1024:.2f}GB")
    if dry_run:
        return [], total_bytes_processed

    rows = list(result)
    if not rows:
        logger.warning(f"Cannot get metrics for table: {tbl.full_table_id}, query: {job.query}")
        return [], total_bytes_processed

    def _to_metric(_name: str, _value: Any) -> Metric:
        metric_name, col_name = column_name_to_metric_target(_name)
        if type(_value) in (datetime.datetime, datetime.date):
            str_value = _value.isoformat()
        else:
            str_value = str(_value)
        return Metric(full_name=f"{tbl.project}.{tbl.dataset_id}.{tbl.table_id}",
                      project_name=tbl.project,
                      dataset_name=tbl.dataset_id,
                      table_name=tbl.table_id,
                      column_name=col_name,
                      metric_name=metric_name,
                      value=str_value,
                      value_type=python_type_to_bigquery_type(type(_value)))

    return [_to_metric(metric_name, metric_value) for metric_name, metric_value in rows[0].items()
           ], total_bytes_processed


def _run_query_job(dry_run: bool, maximum_bytes_billed: Optional[int], query_builder: BigQueryProfileQueryBuilder,
                   tbl: bigquery.Table) -> tuple[bigquery.QueryJob, bigquery.client.RowIterator]:
    query = query_builder.build_query_for_table_profile(tbl)
    bq_cli = bigquery.Client(project=tbl.project)
    jc = bigquery.QueryJobConfig()
    if maximum_bytes_billed:
        jc.maximum_bytes_billed = maximum_bytes_billed
    jc.use_legacy_sql = False
    jc.dry_run = dry_run
    job = bq_cli.query(query, jc)
    result = job.result()
    return job, result
