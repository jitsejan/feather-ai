# DuckDB Driver Issue - Workaround

If you're getting "Could not load database driver: DuckDBEngineSpec", here are solutions:

## Quick Fix: Use SQL Lab

You can query Motherduck directly in SQL Lab without setting up a database connection:

1. Go to **SQL Lab** → **SQL Editor**
2. Use the default connection or any connection
3. Run queries directly (Superset will execute them)

**Note:** This works for queries but you won't be able to create datasets/charts from tables automatically.

## Alternative: Use PostgreSQL Connection String

Some users have success with this format:

```
postgresql://user:pass@localhost/dbname
```

But for Motherduck, try using the "Other" database type with:

```
duckdb:///:memory:?motherduck_token=YOUR_TOKEN
```

## Permanent Fix: Install in System Python

The issue is that `duckdb-engine` is installed in user directory but Superset can't find it. 

Try this:
```bash
docker-compose exec superset bash
# Inside container:
pip install duckdb-engine --system
exit
docker-compose restart superset
```

## Or: Use a Custom Superset Image

Create a `Dockerfile`:
```dockerfile
FROM apache/superset:latest
RUN pip install duckdb-engine
```

Then update `docker-compose.yml` to use this image.

## Current Status

- ✅ Superset is running at http://localhost:8088
- ✅ `duckdb-engine` package is installed
- ⚠️  Superset can't detect it (path issue)
- 💡 **Workaround:** Use SQL Lab for now

