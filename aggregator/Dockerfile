FROM python:3.11-slim

WORKDIR /app

# Install build dependencies for asyncpg
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# We'll mount src for local dev, but let's copy it just in case
COPY src/ /app/src/

# Non-root user for security (rekomendasi task.md)
RUN useradd -m appuser && chown -R appuser /app
USER appuser

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]
