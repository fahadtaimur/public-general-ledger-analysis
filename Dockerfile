FROM python:3.11-slim

WORKDIR /app
COPY src/ src/
COPY pyproject.toml .
COPY run_pipeline.py .

RUN pip install --no-cache-dir .

CMD ["python", "run_pipeline.py"]
