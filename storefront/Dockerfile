# Storefront image. The code is mounted at /app/storefront by docker-compose, so code
# changes need only a container restart, not a rebuild. Rebuild after editing requirements.txt.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /tmp/storefront-requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/storefront-requirements.txt

CMD ["python", "-m", "uvicorn", "storefront.app.main:app", "--host", "0.0.0.0", "--port", "8010"]
