# Imagen de la capa de SERVING.
#
# Deliberadamente sin JVM ni PySpark: el dashboard sólo lee el artefacto DuckDB
# que produce el pipeline (~25 MB), así que la imagen pesa ~250 MB en vez de los
# ~1,5 GB que costaría empaquetar Spark. El cómputo vive en CI
# (.github/workflows/pipeline.yml) o en una máquina de desarrollo.
#
#   docker build -t retail-lakehouse .
#   docker run -p 8501:8501 retail-lakehouse
#
# Para ejecutar también el pipeline dentro del contenedor, ver
# docs/despliegue.md (requiere una imagen base con JDK 17).

FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONIOENCODING=utf-8 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# ---------------------------------------------------------------------------
FROM base AS deps
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# ---------------------------------------------------------------------------
FROM base AS runtime

# Usuario sin privilegios.
RUN useradd --create-home --uid 10001 appuser

COPY --from=deps /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=deps /usr/local/bin /usr/local/bin

COPY --chown=appuser:appuser app/ ./app/
COPY --chown=appuser:appuser .streamlit/ ./.streamlit/

# Si existe un artefacto local se hornea en la imagen; si no, la app lo descarga
# del release publicado al arrancar (ver app/data.py).
COPY --chown=appuser:appuser data/serving/ ./data/serving/

USER appuser

ENV SERVING_DUCKDB=/app/data/serving/serving.duckdb \
    PIPELINE_ENABLED=0

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')"

ENTRYPOINT ["streamlit", "run", "app/streamlit_app.py", \
            "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
