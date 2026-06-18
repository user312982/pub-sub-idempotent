FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/

# Non-root user for security (rekomendasi task.md)
RUN useradd -m appuser && chown -R appuser /app
USER appuser

CMD ["python", "-m", "src.publisher"]
