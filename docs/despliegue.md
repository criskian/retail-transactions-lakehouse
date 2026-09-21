# Despliegue

La aplicación y el pipeline se despliegan por separado a propósito
([ADR-0006](ADR/0006-computo-y-serving-separados.md)):

| | Dónde corre | Qué necesita |
|---|---|---|
| **Pipeline** | GitHub Actions (o tu máquina) | Python 3.11 + JDK 17 + 6 GB RAM |
| **Dashboard** | Streamlit Cloud / Docker / cualquier host Python | Python 3.11 y 24,5 MB de datos |

El dashboard **nunca importa PySpark**. Si `requirements.txt` acabara
incluyéndolo, el despliegue fallaría por tamaño y por falta de JVM.

---

## 1. Pipeline en GitHub Actions

`.github/workflows/pipeline.yml` se dispara en tres casos: manualmente
(`workflow_dispatch`), al empujar cambios a `data/landing/` o `src/pipeline/`,
y semanalmente.

Qué hace:

1. `checkout` con `lfs: true` y `git lfs pull` — el dataset se versiona con LFS.
2. `setup-java` con Temurin 17. PySpark 3.5 no soporta Java 21+, y los runners
   traen los JDK LTS precacheados.
3. Instala `requirements-pipeline.txt`.
4. `python -m src.pipeline.run --doctor` para fallar temprano si el entorno no
   sirve.
5. bronze → silver → gold → models.
6. `python -m src.pipeline.quality` — 26 contratos sobre la capa Gold. Si algo
   no cuadra, el build falla y **no se publica** nada.
7. `export_serving.py` produce `dist/serving.duckdb`.
8. Se publica como asset del release `serving-latest`.

Los runners de repositorios públicos dan 4 vCPU / 16 GB y minutos ilimitados.
`SPARK_DRIVER_MEMORY=6g` deja margen para la JVM y el sistema.

### Ejecutarlo a mano

Pestaña **Actions → pipeline → Run workflow**. Con
`skip_models: true` sólo recalcula KPIs y agregados, que tarda ~2 minutos.

---

## 2. Dashboard en Streamlit Community Cloud

1. Conecta el repositorio en [share.streamlit.io](https://share.streamlit.io).
2. Fichero principal: `app/streamlit_app.py`.
3. Dependencias: `requirements.txt` (se detecta solo).
4. Variables de entorno, en *Advanced settings → Secrets*:

```toml
GITHUB_REPO = "criskian/retail-transactions-lakehouse"
SERVING_RELEASE_TAG = "serving-latest"
PIPELINE_ENABLED = "0"
```

**Versión de Python.** Streamlit Cloud usa Python 3.14 por defecto. Todas las
dependencias de `requirements.txt` están fijadas a versiones con wheel para 3.11
y 3.14; si alguna no lo tuviera, pip intentaría compilarla, no hay `cmake` en el
entorno y el despliegue fallaría con *Error installing requirements*. El job
`serving-deps` de CI instala el serving en ambas versiones con
`--only-binary=:all:` para detectarlo antes de desplegar.

Al arrancar, `app/data.py` descarga el artefacto del release y lo cachea en
`/tmp` con `@st.cache_resource`. No escribe nada más: el sistema de ficheros del
entorno es efímero.

**Límites del plan gratuito** (pueden cambiar): ~1 GB de RAM y la aplicación
**duerme tras 12 h sin visitas**; el primer acceso la despierta. Conviene
decirlo en el README para que nadie piense que está rota.

`PIPELINE_ENABLED=0` hace que la página "Nuevos datos" pase a modo explicativo
en vez de intentar lanzar Spark.

---

## 3. Docker

La imagen contiene **sólo** la capa de serving (~250 MB):

```bash
docker build -t retail-lakehouse .
docker run -p 8501:8501 retail-lakehouse
```

Si existe `data/serving/serving.duckdb` se hornea en la imagen; si no, la
aplicación lo descarga del release al arrancar.

Para montar el artefacto desde fuera en lugar de copiarlo:

```bash
docker run -p 8501:8501 \
  -v "$(pwd)/data/serving:/app/data/serving:ro" \
  retail-lakehouse
```

### Correr también el pipeline en un contenedor

Requiere una imagen base con JDK 17. No está en el `Dockerfile` principal para
no cuadruplicar su tamaño:

```dockerfile
FROM eclipse-temurin:17-jdk-jammy
RUN apt-get update && apt-get install -y python3.11 python3-pip && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements-pipeline.txt requirements.txt ./
RUN pip install --no-cache-dir -r requirements-pipeline.txt
COPY src/ ./src/
ENTRYPOINT ["python3", "-m", "src.pipeline.run", "--step", "full"]
```

```bash
docker run -v "$(pwd)/data:/app/data" retail-lakehouse-pipeline
```

---

## 4. Otros destinos

**Hugging Face Spaces** acepta el `Dockerfile` tal cual; basta con exponer el
puerto 8501 y declarar `app_port: 8501` en el encabezado del `README` del Space.

**Fly.io / Railway / Render** funcionan con el mismo Dockerfile. Ventaja frente
a Streamlit Cloud: no duerme. Coste: unos pocos dólares al mes.

---

## 5. Comprobaciones antes de publicar

```bash
invoke lint
invoke test
python -m src.pipeline.quality          # 26 contratos sobre Gold
python -m src.pipeline.export_serving   # tamaño esperado ~25 MB
docker build -t retail-lakehouse . && docker run --rm -p 8501:8501 retail-lakehouse
```

Y una lectura rápida: si `requirements.txt` menciona `pyspark`, el despliegue
va a fallar. Es el error más fácil de cometer al añadir una dependencia.
