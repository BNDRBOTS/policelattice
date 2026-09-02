FROM python:3.11-slim

# System dependencies for PDF, OCR, and common utilities
RUN apt-get update && apt-get install -y \
    gcc \
    tesseract-ocr \
    libtesseract-dev \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Poetry
ENV POETRY_VERSION=1.6.1
RUN pip install poetry==$POETRY_VERSION

# Copy dependency files and install Python packages
COPY pyproject.toml poetry.lock* ./
RUN poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi --no-dev

# Copy the rest of the application
COPY . .

# Create an entrypoint script to run database init and start the server
RUN printf '#!/bin/sh\npython -m app.api.scripts.init_db\nexec uvicorn app.api.main:app --host 0.0.0.0 --port "${PORT:-8000}"\n' > /start.sh && chmod +x /start.sh

EXPOSE 8000

CMD ["/start.sh"]
