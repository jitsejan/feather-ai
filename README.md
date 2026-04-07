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
   - Update `secrets.toml` with your Confluence API credentials and MotherDuck connection string
   - Update `config.toml` with your Confluence instance URL, space key, and AI model preferences

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

## API Testing with Bruno

Bruno is an open-source API testing tool. We've included Bruno collection files for testing the Confluence API:

### Setup
1. Install Bruno: `npm install -g @usebruno/cli` or download from https://www.usebruno.com/
2. Import the collection: Open Bruno and import `confluence-api.bru`
3. Load environment: Copy `bruno-environment-template.json` to `bruno-environment.json` and update with your actual credentials

### Usage
- The collection includes a request to fetch Confluence pages with the same parameters as the dlt pipeline
- Update the environment variables with your Confluence credentials
- Run the request to test API connectivity before running the full pipeline

### Environment Variables
- `base_url`: Your Confluence instance URL
- `space_key`: Your Confluence space key
- `username`: Your Confluence email
- `api_token`: Your Confluence API token
