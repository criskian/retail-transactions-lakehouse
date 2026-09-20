> **Documento archivado.** Diapositivas de la propuesta de arquitectura
> (mayo de 2026). Se conserva como registro de la propuesta inicial.

---

# Diapositivas — Propuesta de Arquitectura
## Análisis y Modelado Analítico de Transacciones de Supermercado
**Autores:** Santiago Espinosa · Cristian Molina
**Curso:** Procesamiento Distribuido de Datos — G1 · 22-mayo-2026

> Formato Marp (`marp diapositivas_arquitectura.md --pptx`).
> También se puede copiar cada bloque entre `---` como una diapositiva en PowerPoint / Google Slides.

---

<!-- Slide 1 — Portada -->

# Análisis y Modelado Analítico de Transacciones de Supermercado
### Propuesta de Arquitectura

**Santiago Espinosa · Cristian Molina**
Procesamiento Distribuido de Datos — G1
22 de mayo de 2026

**En una frase:** una solución modular, distribuida y reproducible que ingiere CSV de transacciones, los procesa con PySpark sobre un lakehouse Parquet (Bronze / Silver / Gold) y expone los resultados en un dashboard Streamlit, con re-cálculo automático al llegar datos nuevos.

> 🎤 Speaker notes — Hola, somos Santiago y Cristian. Vamos a presentar la arquitectura con la que vamos a construir el análisis del supermercado. La idea central: no quedarnos en un notebook, entregar una solución funcional que se actualice sola cuando aparezcan archivos nuevos.

---

<!-- Slide 2 — Problema y datos reales -->

# El problema y lo que ya sabemos del dataset

**Objetivo del curso:** analizar comportamiento transaccional sin precios → todas las métricas son **relativas** (volumen, frecuencia, diversidad, recencia).

**Datos en disco (ya inspeccionados):**
- **4 tiendas:** 102, 103, 107, 110 → `XXX_Tran.csv`
- **1.108.987 canastas** (filas)
- Formato: `fecha | tienda | cliente | lista_productos_separados_por_espacio`
- Catálogo: **~95.000 productos** mapeados a **~70 categorías**

**Implicación de diseño:**
> Cada canasta hay que **`explode`** a (transacción, producto). Eso lleva la tabla de 1.1M a **>10M filas** → justifica un motor **distribuido (Spark)** en lugar de pandas.

> 🎤 Speaker notes — El dato clave que marca el diseño: las canastas vienen como una sola fila con la lista de productos pegada con espacios. Cuando la abrimos a formato largo, pasamos de un millón a decenas de millones de filas. Por eso Spark, no pandas. Y como no hay precios, todas las métricas tienen que ser relativas.

---

<!-- Slide 3 — Arquitectura propuesta -->

# Arquitectura propuesta — 5 capas desacopladas

> 🖼 **Insertar aquí el diagrama** `DIAGRAMA ARQUITECTURA.pdf` (la imagen del Drawio).

**Lectura de izquierda a derecha:**

1. **Fuentes** → CSV de transacciones + catálogo, depositados en `data/landing/`.
2. **Capa de Ingesta** → *watcher* + validador de esquema (cuarentena si falla).
3. **Lakehouse local — Parquet (Medallion)** → **Bronze** (raw), **Silver** (limpio + explode + join con catálogo), **Gold** (tablas analíticas).
4. **Capa de Procesamiento — PySpark** → ETL Bronze→Silver, *feature engineering* cliente/producto/día, y modelos **K-Means + FP-Growth + ALS**.
5. **Capa de Servicio** → **Streamlit** dashboard (resumen, visualizaciones y análisis avanzado) + endpoints FastAPI opcionales.

**Principio de diseño:** las capas se comunican sólo por archivos Parquet → podemos cambiar herramientas en una capa sin tocar las demás.

> 🎤 Speaker notes — La arquitectura tiene cinco capas. La clave es el medallion en Parquet: Bronze es raw, Silver ya está explotado y unido con el catálogo, y Gold son las tablas que el dashboard consume directo. Cada capa habla con la siguiente sólo por archivos, así que mañana podemos cambiar Streamlit por otra cosa sin tocar Spark, o cambiar Spark sin tocar el dashboard.

---

<!-- Slide 4 — Stack tecnológico y por qué -->

# Stack tecnológico y justificación

| Capa | Herramienta | Por qué |
|---|---|---|
| Procesamiento | **PySpark 3.5** (modo local) | Eje del curso; escala a cluster sin reescribir |
| Almacenamiento | **Parquet** + medallion Bronze/Silver/Gold | Columnar, comprimido, particionable |
| ML | **Spark MLlib**: KMeans, FP-Growth, ALS | Mantiene el pipeline distribuido extremo a extremo |
| Visualización | **Streamlit** (multipágina) | Entrega "no sólo notebook" — funcional |
| Consulta del dashboard | **DuckDB** sobre Parquet | Lectura analítica sin levantar Spark cada vez |
| Orquestación | **Makefile + CLI Python** | Suficiente; sin sobre-ingeniería |
| Entorno | Python 3.11 + `uv`/`poetry`, Docker opcional | Reproducibilidad |

**Mapeo a los requerimientos del enunciado:**
- Segmentación → **KMeans** sobre `dim_customer_features` (frecuencia, volumen, diversidad, recencia).
- Recomendador producto–producto → **FP-Growth** (reglas de asociación con support/confidence/lift).
- Recomendador cliente–producto → **ALS** (filtrado colaborativo implícito, cantidad como rating).

> 🎤 Speaker notes — Elegimos Spark porque es el eje del curso y porque el volumen lo justifica. Para los modelos nos quedamos dentro de Spark MLlib en lugar de saltar a scikit-learn, así el pipeline es distribuido de punta a punta. Para que el dashboard sea ágil leemos Parquet con DuckDB — no toca levantar un SparkSession sólo para mostrar un gráfico.

---

<!-- Slide 5 — Incorporación de nuevos datos (componente C) -->

# Incorporación de nuevos datos — automática e idempotente

**Flujo cuando llega un archivo nuevo a `data/landing/`:**

1. **Watcher** detecta el archivo y le asigna un `ingest_id`.
2. **Validador de esquema** → si falla, va a `_quarantine/` con su `.error.log`.
3. **Bronze append** particionado por `store_id` y mes.
4. **Hash de fila** como `transaction_id` → **idempotencia**: re-procesar el mismo archivo no duplica.
5. **Silver / Gold overwrite por partición** → sólo se recalcula lo afectado.
6. **Re-entrenamiento** de K-Means / FP-Growth / ALS.
7. **Refresco** automático de la caché que lee el dashboard.

**Lo que esto garantiza:**
- Añadir **una nueva tienda** = soltar un archivo, sin tocar código.
- Añadir **nuevas fechas** = sólo se recalculan esas particiones.
- Re-correr todo es seguro (idempotente).

> 🎤 Speaker notes — Este es el componente C del enunciado y para nosotros es el más importante de la arquitectura. El sistema no necesita intervención: el estudiante o el profesor suelta un CSV en `data/landing/` y el pipeline sólo, valida, particiona, deduplica y refresca el dashboard. Si llega una tienda nueva, no hay que tocar código.

---

<!-- Slide 6 — Plan de trabajo y cierre -->

# Plan de trabajo y entregas

| Fecha | Entrega | Estado |
|---|---|---|
| **22-may** | Propuesta de arquitectura | **Hoy ✅** |
| 29-may | Resumen ejecutivo + visualizaciones analíticas | Pipeline Bronze→Silver→Gold + Streamlit páginas 1 y 2 |
| 05-jun | Código fuente + informe técnico | Repositorio reproducible |
| 09–10-jun | Análisis avanzado | Clustering + recomendador + demo de ingesta |

**Riesgos identificados y cómo los manejamos:**
- Spark local con poca RAM → procesar por particiones + muestras en desarrollo.
- Cardinalidad alta de productos en FP-Growth → filtro por `min_support` + top-N por volumen.
- Cold-start en ALS → fallback a top-N por categoría favorita del clúster del cliente.

### Cierre
> Arquitectura **modular, distribuida, reproducible y funcional**.
> Próximo hito (29-may): demo del Resumen Ejecutivo y Visualizaciones Analíticas ya operativas sobre el lakehouse.

**¿Preguntas?**

> 🎤 Speaker notes — Cerramos con el plan. Hoy entregamos la arquitectura; la próxima semana ya mostramos el pipeline corriendo con el resumen ejecutivo y las visualizaciones analíticas. Identificamos tres riesgos concretos con su mitigación para mostrar que la propuesta es realista, no sólo bonita en el papel. Gracias.
