# Storefront image. The code is mounted at /app/storefront by docker-compose, so code
# changes need only a container restart, not a rebuild. Rebuild after editing requirements.txt.
#
# Two stages (security review 2026-11-11, item 14):
# - runtime: what serves poke6s.com (docker-compose builds this target). Pinned packages only,
#   no test tools.
# - test: runtime plus pytest and httpx, for `make test-storefront`.
# The base image is pinned to a Python patch release and Debian version: bump it on purpose
# (for security fixes, about monthly), rebuild, run the tests, deploy.
FROM python:3.12.16-slim-trixie AS runtime

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt constraints.txt /tmp/storefront/
RUN pip install --no-cache-dir -r /tmp/storefront/requirements.txt -c /tmp/storefront/constraints.txt

CMD ["python", "-m", "uvicorn", "storefront.app.main:app", "--host", "0.0.0.0", "--port", "8010"]

FROM runtime AS test
COPY requirements-dev.txt /tmp/storefront/
RUN pip install --no-cache-dir -r /tmp/storefront/requirements-dev.txt -c /tmp/storefront/constraints.txt
