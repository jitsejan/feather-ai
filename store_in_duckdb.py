import dlt
import logging
import os
import sys
from io import StringIO
import contextlib

# Suppress dlt's run logging before importing pipeline
os.environ['DLT_LOG_LEVEL'] = 'ERROR'

# Suppress all verbose logging from dlt and HTTP libraries
logging.basicConfig(
    level=logging.ERROR,
    format='%(levelname)s [%(filename)s:%(lineno)d] %(message)s'
)

# Suppress specific loggers that are chatty
for logger_name in ['dlt', 'dlt.sources', 'dlt.pipeline', 'dlt.destinations', 
                     'urllib3', 'requests', 'httpx', 'httpcore']:
    logging.getLogger(logger_name).setLevel(logging.ERROR)

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

try:
    from dlt.destinations import motherduck
    MOTHERDUCK_AVAILABLE = True
except ImportError:
    from dlt.destinations import duckdb
    MOTHERDUCK_AVAILABLE = False
from extract_confluence import confluence_source

def _build_pipeline(refresh=None):
    if MOTHERDUCK_AVAILABLE:
        destination = motherduck(dlt.secrets["destination.duckdb.credentials"])
    else:
        destination = duckdb()

    pipeline = dlt.pipeline(
        pipeline_name="confluence_to_motherduck",
        destination=destination,
        dataset_name="confluence_data",
        refresh=refresh,
    )

    # If using regular duckdb, set the connection string
    if not MOTHERDUCK_AVAILABLE:
        pipeline.destination.credentials = dlt.secrets["destination.duckdb.credentials"]

    return pipeline


def _run_pipeline(refresh=None):
    pipeline = _build_pipeline(refresh=refresh)

    # Get the raw pages resource from the source
    pages_resource = confluence_source()

    # Apply transformers
    from extract_confluence import process_pages, process_hierarchy
    process_pages_resource = pages_resource | process_pages
    process_hierarchy_resource = pages_resource | process_hierarchy

    # Load processed pages and hierarchy from Confluence - suppress all output
    print("Loading pages...")
    with contextlib.redirect_stdout(StringIO()), contextlib.redirect_stderr(StringIO()):
        pipeline.run(process_pages_resource, refresh=refresh)

    print("Loading hierarchy...")
    with contextlib.redirect_stdout(StringIO()), contextlib.redirect_stderr(StringIO()):
        pipeline.run(process_hierarchy_resource, refresh=refresh)

    return pipeline


def create_pipeline():
    print("Starting Confluence extraction...")

    try:
        pipeline = _run_pipeline()
    except Exception as e:
        message = str(e)
        if "Adding columns with constraints not yet supported" in message:
            print("Resetting Confluence tables for merge-based upserts...")
            pipeline = _build_pipeline()
            pipeline.drop_pending_packages()
            pipeline = _run_pipeline(refresh="drop_resources")
        else:
            print(f"\n✗ ERROR: {e}")
            logger.error(f"Pipeline failed with error: {e}", exc_info=True)
            raise

    print("\n" + "="*60)
    print("✓ SUCCESS: Data extracted and loaded!")
    print(f"  Dataset: {pipeline.dataset_name}")
    print(f"  Pipeline: {pipeline.pipeline_name}")
    print("="*60)

if __name__ == "__main__":
    create_pipeline()
