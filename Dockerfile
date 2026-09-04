FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV POETRY_VERSION=1.8.4 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir poetry==$POETRY_VERSION

COPY pyproject.toml poetry.lock* ./
RUN poetry install --no-ansi --no-root

COPY . .
RUN poetry install --no-ansi

# Data volume for the SQLite fallback, embedding cache and OCR output.
# On Railway a Postgres plugin supplies DATABASE_URL and this stays unused.
RUN mkdir -p /app/data/models

EXPOSE 8000

CMD ["sh", "-c", "uvicorn app.api.main:app --host 0.0.0.0 --port \"${PORT:-8000}\""]
