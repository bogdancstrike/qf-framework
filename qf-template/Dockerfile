# ============================================================
# qf-template — two-stage Docker build
#
# Stage 1 (builder): install all Python dependencies into a
#   virtual environment so only the venv is copied to the
#   final image — no build tools, no cache, smaller image.
#
# Stage 2 (runtime): minimal Python image with only the venv
#   and application code.  Runs as a non-root user.
#
# Build:
#   docker build -t qf-template:latest .
#
# Run (with .env file):
#   docker run --env-file .env -p 5000:5000 qf-template:latest
# ============================================================

# ── Stage 1: builder ────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# Install build-time system deps (psycopg2 needs libpq-dev)
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create venv
RUN python -m venv /build/venv
ENV PATH="/build/venv/bin:$PATH"

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Install the QF framework from the parent directory.
# In CI/CD replace this with:  pip install qf==<version>
COPY ../src /build/qf_framework_src
RUN pip install --no-cache-dir /build/qf_framework_src/../


# ── Stage 2: runtime ────────────────────────────────────────
FROM python:3.12-slim AS runtime

WORKDIR /app

# Runtime system deps only (psycopg2 needs libpq at runtime)
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy venv from builder (no pip, no gcc, no cache)
COPY --from=builder /build/venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

# Copy application source
COPY src/       /app/src/
COPY maps/      /app/maps/
COPY main.py    /app/main.py

# Non-root user for security
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Health check — Docker marks the container healthy once /app/health responds
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5000/app/health')" || exit 1

EXPOSE 5000

CMD ["python", "main.py"]
