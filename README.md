# feather-ai
FeatherAI ingests Confluence data into MotherDuck with `dlt` and enforces a dbt modeling layer for downstream retrieval and LLM workflows.

## Setup

1. Install uv (if not already installed):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. Install dependencies:
   ```bash
   uv sync
   ```

3. Configure your credentials and settings in `.dlt/secrets.toml` and `.dlt/config.toml`:
   - Copy `.dlt/secrets.toml.sample` to `.dlt/secrets.toml` and update it with your Confluence API credentials and MotherDuck connection string
   - Use the official dlt-style source namespace: `[sources.atlassian_confluence]`
   - Update `config.toml` with your Confluence instance URL, space key, Confluence expand scope, and AI model preferences

4. Extract raw data from Confluence into MotherDuck with dlt:
   ```bash
   uv run python store_in_duckdb.py
   ```
   To reset and recrawl raw tables:
   ```bash
   uv run python store_in_duckdb.py --drop-existing
   ```
   This project uses dlt's official REST source pattern (`RESTAPIConfig` + `rest_api_resources`)
   with a dedicated `atlassian_confluence_source` entrypoint so it remains easy to extend with
   additional Confluence endpoints.
   Raw tables are loaded into the `raw_confluence` schema.

5. Configure dbt profile:
   ```bash
   cp dbt/profiles.yml.sample dbt/profiles.yml
   export MOTHERDUCK_CONNECTION_STRING="$(uv run python -c "import dlt; print(dlt.secrets['destination.duckdb.credentials'])")"
   ```

6. Build curated models with dbt (required before embeddings/query):
   ```bash
   uv run dbt run --project-dir dbt --profiles-dir dbt
   uv run dbt test --project-dir dbt --profiles-dir dbt
   ```
   Curated retrieval-ready tables are built in clean dbt schemas (`stg_confluence`,
   `int_confluence`, `analytics`) without target schema prefixes, including:
   - `analytics.dim_confluence_documents`
   - `analytics.fct_confluence_chunks`

7. Generate embeddings from dbt chunks:
   ```bash
   uv run python generate_embeddings.py
   ```

8. Query the data:
   ```bash
   uv run python query.py
   ```
