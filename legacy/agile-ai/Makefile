.PHONY: dbt-run dbt-test dbt-build dbt-debug dbt-clean evidence-dev evidence-build evidence-install all refresh

DBT_PROJECT_DIR := transform
DBT_PROFILES_DIR := transform
DBT_DUCKDB_PATH := $(CURDIR)/jira_pipeline.duckdb
EVIDENCE_DIR := dashboards

dbt-run:
	uv run dbt run --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-test:
	uv run dbt test --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-build:
	uv run dbt build --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-debug:
	uv run dbt debug --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-clean:
	uv run dbt clean --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-deps:
	uv run dbt deps --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-compile:
	uv run dbt compile --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-docs-generate:
	uv run dbt docs generate --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

dbt-docs-serve:
	uv run dbt docs serve --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

# Evidence dashboard commands
evidence-install:
	cd $(EVIDENCE_DIR) && npm install

evidence-sources:
	cd $(EVIDENCE_DIR) && npm run sources

evidence-dev:
	cd $(EVIDENCE_DIR) && npm run sources && npm run dev

evidence-build:
	cd $(EVIDENCE_DIR) && npm run sources && npm run build

evidence-preview:
	cd $(EVIDENCE_DIR) && npm run preview

# Superset commands
superset-up:
	docker-compose up -d superset
	@echo "✅ Superset starting at http://localhost:8088"
	@echo "   Wait ~60 seconds for initialization, then run: make superset-init"

superset-down:
	docker-compose down

superset-logs:
	docker-compose logs -f superset

superset-init:
	docker-compose exec superset superset db upgrade
	docker-compose exec superset superset fab create-admin \
		--username admin \
		--firstname Admin \
		--lastname User \
		--email admin@example.com \
		--password admin || true
	docker-compose exec superset superset init
	@echo "✅ Superset initialized! Access at http://localhost:8088 (admin/admin)"

superset-restart:
	docker-compose restart superset

superset-import:
	@echo "Importing dashboards and creating datasets..."
	@uv run python superset/import_dashboards.py || echo "💡 Make sure Superset is running and database is connected"

superset-fix-charts:
	@echo "Fixing chart configurations..."
	@uv run python superset/fix_charts.py || echo "💡 Make sure Superset is running"

superset-fix-dashboards:
	@echo "Fixing dashboard layouts..."
	@uv run python superset/fix_dashboard_layout.py || echo "💡 Make sure Superset is running"

superset-rebuild-dashboards:
	@echo "Rebuilding dashboards from scratch..."
	@uv run python superset/rebuild_dashboards.py || echo "💡 Make sure Superset is running"

superset-fix-viz-types:
	@echo "Fixing chart visualization types..."
	@uv run python superset/fix_chart_viz_types.py || echo "💡 Make sure Superset is running"

superset-fix-all-charts:
	@echo "⚠️  This will DELETE and RECREATE all charts with proper config..."
	@uv run python superset/fix_all_charts.py || echo "💡 Make sure Superset is running"

# Convenience commands
all: dbt-run evidence-dev

refresh:
	uv run jira_pipeline
	$(MAKE) dbt-run
	@echo "✅ Data refreshed! Reload your Evidence dashboard in the browser."

dashboard: evidence-dev

help:
	@echo "🚀 Agile AI - Available Commands"
	@echo ""
	@echo "📊 Evidence Dashboard:"
	@echo "  make evidence-install   - Install Evidence dependencies"
	@echo "  make evidence-sources   - Build Evidence data sources"
	@echo "  make evidence-dev       - Start Evidence dev server (http://localhost:3000)"
	@echo "  make evidence-build     - Build static Evidence site"
	@echo "  make evidence-preview   - Preview production build"
	@echo "  make dashboard          - Alias for evidence-dev"
	@echo ""
	@echo "🔄 dbt Commands:"
	@echo "  make dbt-run           - Run all dbt models"
	@echo "  make dbt-test          - Run dbt tests"
	@echo "  make dbt-build         - Build and test models"
	@echo "  make dbt-debug         - Debug dbt configuration"
	@echo "  make dbt-clean         - Clean dbt artifacts"
	@echo ""
	@echo "📈 Superset Dashboard:"
	@echo "  make superset-up       - Start Superset (Docker)"
	@echo "  make superset-down     - Stop Superset"
	@echo "  make superset-init     - Initialize Superset (first time)"
	@echo "  make superset-logs     - View Superset logs"
	@echo "  make superset-restart  - Restart Superset"
	@echo ""
	@echo "⚡ Convenience:"
	@echo "  make refresh           - Fetch Jira data + run dbt"
	@echo "  make all               - Run dbt + start Evidence"
	@echo "  make help              - Show this help message"
