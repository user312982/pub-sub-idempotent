FROM python:3.11-slim

WORKDIR /app

RUN adduser --disabled-password --gecos '' appuser && \
    mkdir -p /app/data && \
    chown -R appuser:appuser /app

USER appuser

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

EXPOSE 8080

ENV PATH=/home/appuser/.local/bin:$PATH

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]
