import dlt
import argparse
import contextlib
import logging
import os
from io import StringIO

logger = logging.getLogger(__name__)

try:
    from dlt.destinations import motherduck
    MOTHERDUCK_AVAILABLE = True
except ImportError:
    from dlt.destinations import duckdb
    MOTHERDUCK_AVAILABLE = False
from extract_confluence import atlassian_confluence_source


def configure_logging(log_level: str = "INFO", dlt_log_level: str = "WARNING") -> None:
    os.environ["DLT_LOG_LEVEL"] = dlt_log_level.upper()
    level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] %(message)s",
    )

    # Keep noisy libraries slightly quieter than the app logger.
    noisy_level = max(level, logging.WARNING)
    for logger_name in [
        "dlt",
        "dlt.sources",
        "dlt.pipeline",
        "dlt.destinations",
        "urllib3",
        "requests",
        "httpx",
        "httpcore",
    ]:
        logging.getLogger(logger_name).setLevel(noisy_level)


def _build_pipeline(refresh=None):
    logger.debug("Building pipeline with refresh=%s", refresh)
    if MOTHERDUCK_AVAILABLE:
        logger.debug("Using motherduck destination")
        destination = motherduck(dlt.secrets["destination.duckdb.credentials"])
    else:
        logger.debug("Using duckdb destination")
        destination = duckdb()

    pipeline = dlt.pipeline(
        pipeline_name="confluence_to_motherduck",
        destination=destination,
        dataset_name="raw_confluence",
        refresh=refresh,
    )

    # If using regular duckdb, set the connection string
    if not MOTHERDUCK_AVAILABLE:
        pipeline.destination.credentials = dlt.secrets["destination.duckdb.credentials"]

    return pipeline


def build_pipeline(refresh=None):
    """Public pipeline factory for orchestrators (Dagster, scripts)."""
    return _build_pipeline(refresh=refresh)


@dlt.source
def confluence_processed_source():
    """Expose processed Confluence resources as a dlt source."""
    source = atlassian_confluence_source()
    pages_resource = source.pages
    from extract_confluence import process_pages, process_hierarchy
    return pages_resource | process_pages, pages_resource | process_hierarchy


def _run_pipeline(refresh=None):
    pipeline = build_pipeline(refresh=refresh)
    processed_source = confluence_processed_source()

    # Load all derived resources in one dlt run so extraction happens once.
    logger.info("Loading pages and hierarchy")
    with contextlib.redirect_stdout(StringIO()), contextlib.redirect_stderr(StringIO()):
        load_info = pipeline.run(processed_source, refresh=refresh)
    logger.info("Load info: %s", load_info)

    return pipeline


def create_pipeline(drop_existing: bool = False):
    logger.info("Starting Confluence extraction")

    try:
        refresh_mode = "drop_resources" if drop_existing else None
        pipeline = _run_pipeline(refresh=refresh_mode)
    except Exception as e:
        message = str(e)
        if "Adding columns with constraints not yet supported" in message:
            logger.warning("Resetting Confluence tables for merge-based upserts")
            pipeline = _build_pipeline()
            pipeline.drop_pending_packages()
            pipeline = _run_pipeline(refresh="drop_resources")
        else:
            logger.exception("Pipeline failed with error: %s", e)
            raise

    logger.info("Data extracted and loaded successfully")
    logger.info("Dataset: %s", pipeline.dataset_name)
    logger.info("Pipeline: %s", pipeline.pipeline_name)


def _parse_args():
    parser = argparse.ArgumentParser(description="Run Confluence dlt pipeline")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Application log level",
    )
    parser.add_argument(
        "--dlt-log-level",
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="dlt/internal library log level",
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop existing raw resources before loading",
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = _parse_args()
    configure_logging(log_level=args.log_level, dlt_log_level=args.dlt_log_level)
    create_pipeline(drop_existing=args.drop_existing)
