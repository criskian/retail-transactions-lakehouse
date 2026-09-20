# Análisis y Modelado Analítico de Transacciones de Supermercado
## Propuesta de Arquitectura — Entrega Parcial (22 de mayo de 2026)

**Curso:** Procesamiento Distribuido de Datos — G1
**Autores:** Santiago Espinosa · Cristian Molina
**Versión:** 1.1 — revisada en septiembre de 2026

> **Estado de este documento.** Es la propuesta original de arquitectura, con
> las cifras del dataset corregidas y las rutas actualizadas a la
> implementación final. Lo que la propuesta describía y **no** se implementó
> está marcado como tal en la sección 9. Las decisiones que sí se tomaron, con
> sus renuncias, están en [`ADR/`](ADR/).

---

## 1. Resumen ejecutivo

Se propone una solución de datos modular, ejecutable de extremo a extremo, capaz de ingerir nuevos archivos de transacciones de supermercado, procesarlos de forma distribuida con **Apache Spark (PySpark)**, materializar tablas analíticas en formato **Parquet** bajo una **arquitectura medallion** (Bronze / Silver / Gold), y exponer los resultados en un **dashboard web Streamlit** que cubre el Resumen Ejecutivo, las Visualizaciones Analíticas y el Análisis Avanzado (segmentación K-Means y recomendador FP-Growth/ALS). La solución no se queda en consola ni en notebook: se entrega como aplicación funcional reproducible localmente.

---

## 2. Contexto y caracterización de los datos

El dataset entregado se compone de dos dominios:

| Dominio | Archivo | Registros | Esquema observado | Separador |
|---|---|---|---|---|
| Catálogo | `Products/Categories.csv` | **50** | `category_id \| category_name` | `\|` |
| Catálogo | `Products/ProductCategory.csv` | **112.010** mapeos de **69.891** productos | `product_id \| category_id` | `\|` |
| Transacciones | `Transactions/102_Tran.csv` | 314.286 | `fecha \| tienda \| cliente \| lista_productos` | `\|` |
| Transacciones | `Transactions/103_Tran.csv` | 407.130 | idem | `\|` |
| Transacciones | `Transactions/107_Tran.csv` | 254.633 | idem | `\|` |
| Transacciones | `Transactions/110_Tran.csv` | 132.938 | idem | `\|` |
| **Total transacciones** | | **1.108.987 canastas** | | |

**Particularidades relevantes para el diseño:**

- Cada fila representa **una canasta** (cliente × tienda × fecha): el ID de transacción es implícito y se construye en el ETL.
- El último campo es una **lista de IDs de producto separada por espacios** que debe ser *explotada* a formato largo. Tras el `explode` la tabla pasa de ~1.1M filas a un orden estimado de **decenas de millones de (transacción, producto)**, lo que **justifica un motor distribuido**.
- **No hay precios ni montos**. Todas las métricas son relativas: volumen (unidades), frecuencia, diversidad, recencia.
- La fecha mínima observada es `2013-01-01`. El `store_id` también aparece en el nombre del archivo, lo que se usará como llave de partición.
- No hay columna de cantidad: si un producto aparece *N* veces en la lista de una canasta, se interpreta como *N* unidades compradas.

---

## 3. Requerimientos

### 3.1 Funcionales (RF)

| ID | Requerimiento | Origen |
|---|---|---|
| RF-1 | Calcular total de unidades vendidas y número de transacciones | Resumen Ejecutivo |
| RF-2 | Top 10 productos y top 10 clientes por volumen / frecuencia | Resumen Ejecutivo |
| RF-3 | Días pico de compra (serie de tiempo + heatmap) | Resumen Ejecutivo |
| RF-4 | Categorías más "rentables" (proxy por volumen/frecuencia) | Resumen Ejecutivo |
| RF-5 | Serie de tiempo de ventas, boxplot por cliente/categoría, heatmap de correlaciones | Visualizaciones Analíticas |
| RF-6 | Segmentación de clientes con K-Means | Análisis Avanzado (A) |
| RF-7 | Recomendador producto–producto y cliente–producto | Análisis Avanzado (B) |
| RF-8 | Re-ejecución automática del pipeline al incorporar nuevos archivos | Análisis Avanzado (C) |

### 3.2 No funcionales (RNF)

- **Escalabilidad:** el ETL debe soportar volúmenes crecientes sin reescribirse → motor distribuido.
- **Modularidad:** capas desacopladas (ingesta, procesamiento, analítica, presentación) para que la incorporación de nuevas fuentes solo toque la capa de ingesta.
- **Reproducibilidad:** entorno declarado (`pyproject.toml` / `requirements.txt`), comando único de ejecución, opcionalmente Dockerfile.
- **Idempotencia:** re-procesar el mismo archivo no debe duplicar registros (se usa `MERGE` lógico por hash de fila o sobreescritura particionada).
- **Observabilidad mínima:** registro por etapa y una entrada por corrida —incluidas las fallidas— en `data/landing/_runs.jsonl`.
- **Portabilidad:** ejecutable en una laptop (modo local de Spark) sin requerir cluster.

---

## 4. Arquitectura propuesta

### 4.1 Vista lógica (alto nivel)

```mermaid
flowchart LR
    subgraph SRC["Fuentes"]
        A1[Archivos CSV de transacciones<br/>por tienda]
        A2[Catálogo productos + categorías]
        A3[Drop zone:<br/>data/landing/]
    end

    subgraph ING["Capa de Ingesta"]
        B1[Watcher / scheduler]
        B2[Validador de esquema]
    end

    subgraph LAKE["Lakehouse local - Parquet"]
        C1[(Bronze<br/>raw fiel a la fuente)]
        C2[(Silver<br/>limpio + explode + joins)]
        C3[(Gold<br/>tablas analíticas)]
    end

    subgraph PROC["Capa de Procesamiento - PySpark"]
        D1[ETL bronze→silver]
        D2[Feature engineering<br/>cliente / producto / día]
        D3[Modelos:<br/>KMeans + FP-Growth + ALS]
    end

    subgraph SERVE["Capa de Servicio"]
        E1[Streamlit dashboard]
        E2[Endpoints de recomendación<br/>opcional FastAPI]
    end

    A1 --> A3
    A2 --> A3
    A3 --> B1 --> B2 --> C1
    C1 --> D1 --> C2
    C2 --> D2 --> C3
    C2 --> D3 --> C3
    C3 --> E1
    C3 --> E2
```

### 4.2 Vista de despliegue (entorno local del estudiante)

```mermaid
flowchart TB
    subgraph HOST["Laptop / Workstation"]
        subgraph PY["Entorno Python 3.11"]
            P1[PySpark 3.5 modo local]
            P2[scikit-learn / mlxtend<br/>fallback no distribuido]
            P3[Streamlit]
        end
        FS[(Sistema de archivos<br/>data/ landing bronze silver gold)]
        LOG[(logs/ + run_id)]
    end
    USR[Usuario / profesor] --> P3
    P1 --> FS
    P2 --> FS
    P3 --> FS
    P1 --> LOG
```

### 4.3 Flujo end-to-end de una corrida

```mermaid
sequenceDiagram
    actor U as Usuario
    participant W as Watcher
    participant S as Spark ETL
    participant L as Lakehouse
    participant M as Modelos (KMeans/FP-Growth)
    participant D as Dashboard

    U->>W: Coloca CSV nuevo en data/landing/
    W->>W: Detecta archivo + valida esquema
    W->>S: Lanza job con run_id
    S->>L: Escribe BRONZE (raw particionado por store/fecha)
    S->>S: Explode lista de productos + join catálogo
    S->>L: Escribe SILVER (transacción×producto)
    S->>S: Construye features por cliente / producto / día
    S->>L: Escribe GOLD (tablas analíticas)
    S->>M: Entrena/actualiza KMeans y FP-Growth
    M->>L: Persiste modelos + tablas de recomendaciones
    D->>L: Lee GOLD bajo demanda
    U->>D: Consulta resumen, visualizaciones, recomendaciones
```

---

## 5. Componentes

### 5.1 Capa de ingesta
- **Drop zone:** carpeta `data/landing/` donde se depositan nuevos archivos (transacciones o catálogo).
- **Detector de cambios:** script `watch.py` basado en `watchdog` o ejecución manual `make ingest`; cada archivo recibe un `ingest_id` y se mueve a `data/landing/_processed/` al finalizar.
- **Validador de esquema:** verifica separador `|`, conteo de columnas, parseabilidad de fecha y de la lista de productos. Archivos inválidos van a `data/landing/_quarantine/` con un `.error.log`.

### 5.2 Capa de almacenamiento (Lakehouse medallion)
- **Bronze** — copia fiel del CSV en Parquet, particionado por `store_id` y `year=`/`month=`. Columnas: `date, store_id, customer_id, product_list_raw, source_file, ingest_ts`.
- **Silver** — tabla larga `transactions_items` tras `explode`: `transaction_id (hash de fila bronze), date, store_id, customer_id, product_id, category_id, category_name, qty`.
- **Gold** — *data marts* listos para consumo:
  - `fact_sales_daily` (date, store_id, units, txn_count, customers)
  - `dim_customer_features` (customer_id, frequency, units_total, distinct_products, distinct_categories, recency_days, avg_basket_size)
  - `dim_product_features` (product_id, category_id, units_total, txn_count, distinct_customers)
  - `cluster_assignments` (customer_id, cluster_id)
  - `product_rules` (antecedent_ids, antecedent_label, antecedent_size, consequent_product_id, support, confidence, lift)
  - `customer_recommendations` (customer_id, product_id, score, rank)
  - `cluster_pca` (proyección 2D de los clusters + centroides)
  - Marts de *serving* (`fact_product_daily`, `fact_category_daily`, `fact_customer_daily`, `dim_customer_top_products`) que evitan que el dashboard consulte Silver — ver [ADR-0006](ADR/0006-computo-y-serving-separados.md)

### 5.3 Capa de procesamiento (PySpark)
Cada paso es un módulo independiente bajo `src/pipeline/` y se invoca por CLI:

```
python -m src.pipeline.run --step bronze
python -m src.pipeline.run --step silver
python -m src.pipeline.run --step gold
python -m src.pipeline.run --step models
python -m src.pipeline.run --step full    # encadena todo + export
python -m src.pipeline.run --doctor       # diagnostica Java / Hadoop / workers
```

Justificación del motor distribuido: tras el `explode` de la lista de productos Silver llega a **10.591.792 filas**; Spark permite procesarlas con SQL declarativo, soporta crecimiento futuro a cluster sin tocar el código de negocio, y es la herramienta central del curso.

### 5.4 Capa de modelos analíticos

| Modelo | Algoritmo | Librería | Entrada | Salida |
|---|---|---|---|---|
| Segmentación | **K-Means** | `pyspark.ml.clustering.KMeans` | `dim_customer_features` escalada | `cluster_assignments` + perfiles de clúster |
| Recomendador por canasta | **FP-Growth** (reglas de asociación) | `pyspark.ml.fpm.FPGrowth` | canastas desde Silver | reglas `antecedent → consequent` con `support, confidence, lift` |
| Recomendador cliente-producto | **ALS** (filtrado colaborativo implícito) | `pyspark.ml.recommendation.ALS` | matriz cliente×producto con `qty` como rating implícito | top-N productos por cliente |

Se selecciona `k` óptimo en K-Means mediante método del codo + silhouette sobre una muestra. Para FP-Growth se reportan reglas con `min_support` y `min_confidence` configurables.

### 5.5 Capa de servicio / visualización
- **Streamlit** como aplicación principal (`app/streamlit_app.py`) con tres páginas:
  1. **Resumen Ejecutivo** — KPIs e indicadores, Top-10, días pico, categorías.
  2. **Visualizaciones Analíticas** — serie de tiempo, boxplots, heatmap de correlaciones.
  3. **Segmentación** — proyección PCA 2D de los clústeres (implementada como *small multiples*, un panel por segmento), perfiles en radar y buscador por cliente.
  4. **Recomendador** — "dado un cliente → recomienda" (ALS) y "dado un producto → qué se compra con él" (FP-Growth).
  5. **Generación de nuevos resultados** — ingesta incremental.
- Lectura directa de Parquet con `pandas`/`duckdb` para baja latencia en el dashboard. El usuario no necesita levantar Spark para *ver* resultados.

### 5.6 Capa de orquestación
Pipeline ejecutable con un `Makefile` (`make ingest`, `make pipeline`, `make app`) y un único punto de entrada en Python. Cada etapa registra duración y conteos en `data/landing/_runs.jsonl`.

---

## 6. Estrategia para la incorporación de nuevos datos (RF-8)

```mermaid
flowchart LR
    N[Archivo nuevo en<br/>data/landing/] --> V{Esquema válido?}
    V -- No --> Q[data/landing/_quarantine/]
    V -- Sí --> H[Hash + ingest_id]
    H --> B[Append a BRONZE<br/>particionado]
    B --> I{Idempotencia:<br/>hash ya existe?}
    I -- Sí --> SKIP[Skip + log]
    I -- No --> R[Re-ejecuta SILVER + GOLD<br/>solo particiones afectadas]
    R --> RM[Re-entrena modelos<br/>incremental o full]
    RM --> NOTIF[Refresca caché del<br/>dashboard]
```

**Principios:**
- **Append-only** en Bronze, **overwrite por partición** en Silver/Gold para evitar duplicados.
- **Procesamiento incremental** por particiones de fecha/tienda cuando es posible.
- **Re-entrenamiento** de modelos: full por ahora (los volúmenes lo permiten); preparado para warm-start de K-Means en versiones futuras.
- La adición de una **nueva tienda** (archivo `XXX_Tran.csv`) o de **nuevas fechas** no requiere cambios de código.

---

## 7. Stack tecnológico y justificación

| Capa | Herramienta | Por qué |
|---|---|---|
| Procesamiento | **PySpark 3.5** (modo local) | Eje del curso; escala a cluster sin reescribir; SQL declarativo |
| Almacenamiento | **Parquet** + medallion | Columnar, comprimido, particionable; estándar de lakehouse |
| ML | **Spark MLlib** (KMeans, FP-Growth, ALS) | Mantiene el pipeline distribuido extremo a extremo |
| Visualización | **Streamlit** | Entrega "no sólo notebook"; iteración rápida; multipágina nativa |
| Consulta dashboard | **DuckDB** sobre Parquet | Lectura analítica sin levantar Spark cada vez |
| Orquestación | **Makefile + CLI Python** | Suficiente para alcance del curso; sin sobre-ingeniería |
| Entorno | **Python 3.11 + uv/poetry** | Reproducible |
| Empaquetado opcional | **Docker** | Portabilidad para sustentación |

**Alternativas consideradas y descartadas:**
- *Pandas puro:* simple pero no defendible en un curso de procesamiento distribuido y frágil ante crecimiento del dataset.
- *Airflow / Prefect:* aporta poco frente al alcance académico y suma fricción operativa.
- *Postgres como gold:* añade un servicio sin beneficio claro; Parquet + DuckDB cubre el caso.

---

## 8. Modelo de datos (Gold — diagrama lógico)

```mermaid
erDiagram
    FACT_SALES_DAILY {
        date day
        int store_id
        bigint units
        bigint txn_count
        bigint customers
    }
    DIM_CUSTOMER_FEATURES {
        int customer_id PK
        bigint frequency
        bigint units_total
        int distinct_products
        int distinct_categories
        int recency_days
        float avg_basket_size
    }
    DIM_PRODUCT_FEATURES {
        int product_id PK
        int category_id
        bigint units_total
        bigint txn_count
        int distinct_customers
    }
    CLUSTER_ASSIGNMENTS {
        int customer_id FK
        int cluster_id
    }
    PRODUCT_RECOMMENDATIONS {
        int product_id FK
        int recommended_product_id
        float score
        float support
        float confidence
        float lift
    }
    DIM_CUSTOMER_FEATURES ||--|| CLUSTER_ASSIGNMENTS : "tiene"
    DIM_PRODUCT_FEATURES ||--o{ PRODUCT_RECOMMENDATIONS : "produce"
```

---

## 9. Estructura del repositorio

> Lo que sigue era el plan inicial. La estructura final está en el README. Tres
> piezas de este esquema **no se implementaron**: `src/features/` y `src/viz/`
> (su lógica vive en `gold.py` y en `app/theme.py`), y `watch.py` (la detección
> de cambios es explícita vía `invoke ingest`, no un *watcher* en segundo
> plano). `_processed/` tampoco existe: los ficheros se quedan en el landing y
> el manifest de hashes decide qué reprocesar.

```
proyecto/
├── data/
│   ├── landing/            # drop zone (entrada)
│   │   ├── _processed/
│   │   └── _quarantine/
│   ├── bronze/             # Parquet raw
│   ├── silver/             # Parquet limpio + explotado
│   └── gold/               # Marts analíticas
├── docs/
│   ├── arquitectura.md     # este documento
│   └── informe_tecnico.md  # entrega final
├── src/
│   ├── pipeline/
│   │   ├── ingest.py
│   │   ├── bronze.py
│   │   ├── silver.py
│   │   ├── gold.py
│   │   ├── models.py
│   │   └── run.py          # CLI
│   ├── features/
│   └── viz/
├── app/
│   └── streamlit_app.py
├── tests/
├── logs/
├── pyproject.toml
├── Makefile
└── README.md
```

---

## 10. Plan de trabajo alineado a entregas

| Fecha | Entrega | Avance esperado |
|---|---|---|
| 22-may-2026 | Arquitectura | Este documento aprobado |
| 29-may-2026 | Resumen ejecutivo + visualizaciones analíticas | Pipeline Bronze→Silver→Gold operativo + Streamlit páginas 1 y 2 |
| 05-jun-2026 | Código fuente + informe técnico | Repositorio completo y reproducible |
| 09/10-jun-2026 | Análisis avanzado | Páginas de clustering, recomendador y demo de ingesta de nuevos datos |

---

## 11. Riesgos y mitigaciones

| Riesgo | Impacto | Mitigación |
|---|---|---|
| Spark local con poca RAM | Pipeline se cae con dataset completo | Configurar `spark.driver.memory`, procesar por particiones (store_id), usar muestras durante desarrollo |
| Cardinalidad alta de productos (~95k) en FP-Growth | Reglas explotan combinatorialmente | Filtrar por `min_support` y por top-N productos por volumen antes de minar |
| Cold-start en ALS (clientes/productos nuevos) | Recomendaciones vacías | Fallback a top-N por categoría favorita del clúster del cliente |
| Esquema inestable en nuevos archivos | Job se rompe | Validador en ingesta + cuarentena, no se contamina Bronze |
| Re-procesamiento duplica datos | Métricas infladas | Bronze append + Silver/Gold overwrite por partición + hash de fila como `transaction_id` |

---

## 12. Conclusión

La arquitectura propuesta es **modular, distribuida, reproducible y completamente funcional** (no se limita a un notebook). Separa con claridad ingesta, almacenamiento, procesamiento, modelado y presentación, lo que permite que la incorporación de nuevos archivos de transacciones dispare automáticamente el recálculo de KPIs, la actualización de los modelos de segmentación y recomendación, y el refresco del dashboard que verá el usuario final.
