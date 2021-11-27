import uuid
from datetime import datetime
from pathlib import Path

import typer
from loguru import logger

from bqdataprofiler import __version__
from bqdataprofiler.models.result import ProfilingResult
from bqdataprofiler.models.setting import load_setting_from_yaml
from bqdataprofiler.profiler import run_profile

app = typer.Typer()


@app.command()
def version():
    """
    Show current version
    """
    print(__version__)


@app.command()
def profile(config: Path = typer.Option(...),
            outdir: Path = typer.Option(None),
            dry_run: bool = typer.Option(False),
            log_error: bool = typer.Option(False)):
    """
    Run profile
    """
    logger.info("Start bigquery-data-profiler")
    logger.info(f"Args: config={config} outdir={outdir} dry_run={dry_run} log_error={log_error}")
    setting = load_setting_from_yaml(config)
    result = ProfilingResult(start_datetime=datetime.now(), run_id=str(uuid.uuid4()))

    logger.info("Start profiling")
    run_profile(setting, result, dry_run)
    logger.info("End profiling")

    if result.errors:
        logger.error(f"{len(result.errors)} errors have occurred!")
        if log_error:
            for e in result.errors:
                logger.error(e.json())

    out_fpath = outdir / "profiling_result.json"

    logger.info("Start writing output")
    if not dry_run:
        with out_fpath.open("w") as f:
            f.write(result.json(indent=2, ensure_ascii=False))
    else:
        logger.info(result.json(indent=2, ensure_ascii=False))
    logger.info("End writing output")
    logger.info("End bigquery-data-profiler")


if __name__ == '__main__':
    app()
