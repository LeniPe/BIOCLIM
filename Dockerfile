FROM python:3.12-slim

# Prevent interactive apt
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies required by rasterio/GDAL
RUN apt-get update && apt-get install -y \
    libexpat1 \
    libgdal-dev \
    gdal-bin \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy dependency files first (better cache)
COPY pyproject.toml uv.lock ./

# Install python deps
RUN uv sync --locked

# Copy application code
COPY src/ src/
COPY main.py .

CMD ["uv", "run", "main.py"]