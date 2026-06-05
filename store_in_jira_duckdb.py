import dlt
import argparse
import contextlib
import logging
import os
from io import StringIO

from dlt.destinations import duckdb

from extract_jira import jira_source, process_issues

logger = logging.getLogger(__name__)


def configure_logging(log_level: str = "INFO", dlt_log_level: str = "WARNING") -> None:
    os.environ["DLT_LOG_LEVEL"] = dlt_log_level.upper()
    level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s",
    )
    noisy_level = max(level, logging.WARNING)
    for name in ["dlt", "dlt.sources", "dlt.pipeline", "dlt.destinations", "urllib3", "requests"]:
        logging.getLogger(name).setLevel(noisy_level)


def build_pipeline(refresh=None):
    try:
        credentials = dlt.secrets["destination.duckdb.credentials"]
    except KeyError:
        credentials = "int.duckdb"
    return dlt.pipeline(
        pipeline_name="jira_to_duckdb",
        destination=duckdb(credentials),
        dataset_name="raw_jira",
        refresh=refresh,
    )


@dlt.source
def jira_processed_source():
    issues_resource = jira_source().issues
    return issues_resource | process_issues


def create_pipeline(drop_existing: bool = False):
    logger.info("Starting Jira extraction")
    refresh = "drop_resources" if drop_existing else None
    pipeline = build_pipeline(refresh=refresh)
    with contextlib.redirect_stdout(StringIO()), contextlib.redirect_stderr(StringIO()):
        load_info = pipeline.run(jira_processed_source(), refresh=refresh)
    logger.info("Load info: %s", load_info)
    logger.info("Data extracted and loaded successfully")
    logger.info("Dataset: %s", pipeline.dataset_name)
    logger.info("Pipeline: %s", pipeline.pipeline_name)
    return pipeline


def _parse_args():
    parser = argparse.ArgumentParser(description="Run Jira dlt pipeline")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    parser.add_argument("--dlt-log-level", default="WARNING",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    parser.add_argument("--drop-existing", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    configure_logging(log_level=args.log_level, dlt_log_level=args.dlt_log_level)
    create_pipeline(drop_existing=args.drop_existing)
