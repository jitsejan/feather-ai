# feather-ai
FeatherAI is a local-first knowledge engine that ingests Confluence data, structures it in DuckDB, and enables semantic search and indexing using local LLMs.

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
   - Update `config.toml` with your Confluence instance URL, space key, Confluence expand scope, and AI model preferences

4. Extract data from Confluence and load into MotherDuck using dlt:
   ```bash
   uv run python store_in_duckdb.py
   ```

5. Generate embeddings:
   ```bash
   uv run python generate_embeddings.py
   ```

6. Query the data:
   ```bash
   uv run python query.py
   ```
