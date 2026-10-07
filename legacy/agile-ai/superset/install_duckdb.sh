#!/bin/bash
# Script to install DuckDB support in Superset
# Run this after Superset is started: docker-compose exec superset bash /app/superset/install_duckdb.sh

echo "Installing duckdb-engine..."
pip3 install --user duckdb-engine

echo "Verifying installation..."
python3 -c "import sys; sys.path.insert(0, '/root/.local/lib/python3.10/site-packages'); import duckdb_engine; print('✅ duckdb-engine installed successfully')" || echo "⚠️  Installation may need Superset restart"

echo ""
echo "✅ Done! You may need to restart Superset:"
echo "   docker-compose restart superset"

