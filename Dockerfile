FROM eclipse-temurin:17-jre-jammy

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip procps \
 && rm -rf /var/lib/apt/lists/*

ENV PYSPARK_PYTHON=python3 \
    RETAIL_LAKEHOUSE_HOME=/data \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY . .

# Bake Delta jars into the image so the first run is fast / offline-capable.
RUN python3 docker/prewarm.py || true

EXPOSE 8000
RUN chmod +x docker/entrypoint.sh
ENTRYPOINT ["docker/entrypoint.sh"]
