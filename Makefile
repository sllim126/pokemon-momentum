test:
	docker-compose exec -T pokemon-momentum python -m unittest /app/tests/test_api_routes.py /app/tests/test_dashboard_frontend.py

test-api:
	docker-compose exec -T pokemon-momentum python -m unittest /app/tests/test_api_routes.py

test-frontend:
	docker-compose exec -T pokemon-momentum python -m unittest /app/tests/test_dashboard_frontend.py

check-fastapi:
	docker-compose exec -T pokemon-momentum python -c "import fastapi; print(fastapi.__version__)"
