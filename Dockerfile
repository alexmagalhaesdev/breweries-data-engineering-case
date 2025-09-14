FROM python:3.12-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates jq bash && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy project files (including prefect.yaml)
COPY app ./app
COPY entrypoint.sh ./entrypoint.sh
COPY prefect.yaml ./prefect.yaml
COPY soda ./soda
COPY scripts ./scripts
COPY tests ./tests

RUN chmod +x /app/entrypoint.sh

CMD ["/app/entrypoint.sh"]
