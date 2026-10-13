FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

RUN apt-get update && apt-get install -y --no-install-recommends openjdk-21-jre-headless && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY mlruns/1/f36d4d33f41b468890ffe5e57cce24dd/artifacts/random_forest_model ./mlruns/1/f36d4d33f41b468890ffe5e57cce24dd/artifacts/random_forest_model

EXPOSE 8000

CMD ["uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8000"]
