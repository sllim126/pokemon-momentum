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

# Storefront (poke6s.com) tests; see storefront/README.md. The image that serves the site has
# no test tools (storefront/Dockerfile), so the tests run in its "test" stage, built from this
# folder's storefront/ and given the code read-only.
.PHONY: test-storefront
test-storefront:
	docker build -q --target test -t poke6s-storefront-test storefront >/dev/null
	docker run --rm --user $$(id -u):$$(id -g) -v $(CURDIR)/storefront:/app/storefront:ro -w /app \
		poke6s-storefront-test python -m pytest -q -p no:cacheprovider storefront/tests
