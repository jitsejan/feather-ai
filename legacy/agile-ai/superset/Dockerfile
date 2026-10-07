FROM apache/superset:latest

# Install DuckDB support in Superset's environment
# Note: Motherduck requires DuckDB 1.4.1 (not 1.4.2)
USER root
RUN pip install --no-cache-dir --target /app/.venv/lib/python3.10/site-packages "duckdb==1.4.1" duckdb-engine || \
    pip install --no-cache-dir "duckdb==1.4.1" duckdb-engine

USER superset

