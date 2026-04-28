DC ?= docker-compose
APP_SERVICE ?= pokemon-momentum

.PHONY: test test-all test-api test-frontend check-fastapi

test:
	$(DC) exec -T $(APP_SERVICE) python tests/run_with_coverage.py

test-all:
	$(DC) exec -T $(APP_SERVICE) python -m pytest -q tests

test-api:
	$(DC) exec -T $(APP_SERVICE) python -m pytest -q tests/test_api_routes.py

test-frontend:
	$(DC) exec -T $(APP_SERVICE) python -m pytest -q tests/test_dashboard_frontend.py

check-fastapi:
	$(DC) exec -T $(APP_SERVICE) python -c "import fastapi; print(fastapi.__version__)"
