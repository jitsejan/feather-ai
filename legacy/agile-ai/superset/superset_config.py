"""
Superset configuration for Agile AI Dashboard
"""
import os

# Database connection string for Motherduck
MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN", "")
MOTHERDUCK_DATABASE = "agile_ai_db"
MOTHERDUCK_SCHEMA = "gold"

# SQLAlchemy connection string for Motherduck
# Note: Don't set this here - configure it manually in Superset UI
# The DuckDB connection will be set up through the UI after Superset starts
# This avoids startup errors if duckdb-engine isn't installed yet

# Feature flags
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_NATIVE_FILTERS_SET": True,
    "ENABLE_DRILL_TO_DETAIL": True,
    "ENABLE_DRILL_BY": True,
}

# Security
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "your-secret-key-change-this-in-production")

# CORS
ENABLE_CORS = True
CORS_OPTIONS = {
    "supports_credentials": True,
    "allow_headers": ["*"],
    "resources": ["*"],
    "origins": ["*"],
}

# Cache
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}

# Language
LANGUAGES = {
    "en": {"flag": "us", "name": "English"},
}

# Timezone
DEFAULT_TIMEZONE = "UTC"

# Row limit
ROW_LIMIT = 10000
SQLLAB_ROW_LIMIT = 10000

# Enable async queries
ENABLE_ASYNC_QUERIES = True

