.PHONY: up down logs register-connectors produce-initial produce-incremental dry-run \
        dbt-parse dbt-run dbt-test diagrams lint yamllint security precommit-install ci-local

# ── Infrastructure ──────────────────────────────────────────────────────────
up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

register-connectors:
	./scripts/register_connectors.sh

# ── Data loading (source Postgres) ──────────────────────────────────────────
produce-initial:
	python3 tests/load_to_postgres.py --data-dir tests/data/ --batch initial --db-url $(DATABASE_URL)

produce-incremental:
	python3 tests/load_to_postgres.py --data-dir tests/data/ --batch incremental --db-url $(DATABASE_URL)

dry-run:
	python3 tests/load_to_postgres.py --data-dir tests/data/ --dry-run

# ── dbt (runs inside the Dagster container, project bind-mounted) ───────────
# `dbt parse` opens no warehouse connection — it is the free validation.
dbt-parse:
	docker compose exec dagster-daemon \
	  bash -c "cd /opt/dagster/dbt && dbt parse --target $${DBT_TARGET:-dev}"

dbt-run:
	docker compose exec dagster-daemon \
	  bash -c "cd /opt/dagster/dbt && dbt run --select silver gold --target $${DBT_TARGET:-dev}"

dbt-test:
	docker compose exec dagster-daemon \
	  bash -c "cd /opt/dagster/dbt && dbt test --select silver gold --target $${DBT_TARGET:-dev}"

# ── Documentation ───────────────────────────────────────────────────────────
# Re-renders assets/*.png from the ```mermaid blocks in README.md (single source).
diagrams:
	python3 scripts/render_diagrams.py

# ── Quality ─────────────────────────────────────────────────────────────────
lint:
	ruff check .

yamllint:
	yamllint connectors/ observability/

security:
	bandit -r dagster/ scripts/ tests/ -ll --skip B101,B608

precommit-install:
	pre-commit install

# Mirrors the shell-syntax and reference checks of .github/workflows/ci.yml,
# so the cheap half of CI can be run before pushing.
ci-local:
	@for f in scripts/*.sh; do echo "bash -n $$f"; bash -n "$$f"; done
	@$(MAKE) lint yamllint
