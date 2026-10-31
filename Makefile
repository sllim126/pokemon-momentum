DC ?= docker-compose
APP_SERVICE ?= pokemon-momentum

.PHONY: test test-all test-api test-frontend check-fastapi

test:
	$(DC) exec -T $(APP_SERVICE) python tests/run_with_coverage.py

test-all:
	$(DC) exec -T $(APP_SERVICE) python -m pytest -q tests

test-api:
	$(DC) exec -T $(APP_SERVICE) python -m pytest -q tests/test_api_routes.py

check-fastapi:
	$(DC) exec -T $(APP_SERVICE) python -c "import fastapi; print(fastapi.__version__)"

# Storefront (poke6s.com) tests run in their own container; see storefront/README.md.
STORE_SERVICE ?= storefront

.PHONY: test-storefront
test-storefront:
	$(DC) exec -T $(STORE_SERVICE) python -m pytest -q -p no:cacheprovider storefront/tests
