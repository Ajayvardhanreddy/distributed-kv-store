FROM python:3.13-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /app

# Copy dependency manifests first (layer-cached until they change)
COPY pyproject.toml uv.lock ./

# Install production dependencies only — no dev extras, no project wheel
RUN uv sync --no-group dev --frozen --no-install-project

# Copy application code
COPY app/ ./app/

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
