> **Documento archivado.** Guion preparado para la sustentación académica de
> junio de 2026. Se conserva como registro; algunas cifras y rutas quedaron
> desactualizadas tras la refactorización de septiembre. Para el estado actual
> del proyecto, ver el [README](../../README.md), el
> [informe técnico](../informe_tecnico.md) y los [ADR](../ADR/).

---

# Guion de sustentación — 10-jun-2026

**Autores:** Santiago Espinosa · Cristian Molina
**Duración objetivo:** 10–15 min

Preparado a partir del feedback de los grupos que ya sustentaron. El profe va a profundizar especialmente en:

1. Cómo funciona K-Means
2. **La estrategia utilizada** (es el bloque más importante)
3. Diagrama de deployment
4. Demostración cargando un dataset nuevo que él entregará

---

## 1) Cómo funciona K-Means — versión "para defender ante el profe"

### El algoritmo en sí (responder esto si pregunta "explíquenme K-Means")

K-Means es un algoritmo de **clustering no supervisado**: no le decimos a qué grupo pertenece cada cliente; el algoritmo descubre los grupos a partir de la geometría de los datos.

Pasos del algoritmo:

1. **Definir k** — el número de clusters que queremos. Es el hiperparámetro principal.
2. **Inicializar k centroides** — k puntos en el espacio de features, elegidos al azar (Spark usa `k-means||`, una variante de `k-means++`, que los siembra de forma más inteligente para que la convergencia sea estable).
3. **Asignar** cada punto al centroide más cercano (distancia euclidiana al cuadrado).
4. **Mover** cada centroide al promedio de los puntos que se le asignaron en el paso anterior.
5. **Repetir** los pasos 3 y 4 hasta que las asignaciones dejen de cambiar o se llegue al `maxIter`. Esto define la convergencia.
6. **Resultado:** cada punto queda con un `cluster_id` y cada cluster tiene un centroide que representa su perfil promedio.

**Lo que K-Means optimiza** es la **suma de distancias cuadradas intra-cluster** (WCSS — *within-cluster sum of squares*). Cada iteración no aumenta esa métrica; por eso converge.

**Limitaciones que el profe puede mencionar y cómo responder:**
- *"K-Means asume clusters esféricos."* — Sí, por eso usamos `StandardScaler` antes: z-score por columna para que las 6 features estén en la misma escala. Sin escalar, `units_total` (rango 1–2860) dominaría a `recency_days` (0–181) y el algoritmo se sesgaría a una sola dimensión.
- *"¿Cómo eligieron k?"* — Por **silhouette score** (ver siguiente sección). No por método del codo, porque WCSS siempre baja con k creciente y elegir el "codo" es subjetivo; silhouette da un número comparable.
- *"¿Y la sensibilidad a la inicialización?"* — Spark usa `k-means||`, fijamos `seed=42` para reproducibilidad y corremos hasta `maxIter=50` con tolerancia 1e-4. Es estable entre ejecuciones (lo verificamos al re-correr el pipeline).

### Lo que hicimos nosotros con K-Means (esto es lo que tienen que saber al dedillo)

- **Tabla de entrada:** `dim_customer_features` — 131.186 filas, una por cliente.
- **Las 6 features** que usamos (saber explicar por qué cada una):
  1. `frequency` — número de transacciones distintas → captura *cuán seguido viene*.
  2. `units_total` — suma de unidades compradas → captura *cuánto compra en total*.
  3. `distinct_products` — diversidad de SKUs → captura *qué tan variada es la compra*.
  4. `distinct_categories` — diversidad de categorías → captura *en cuántos rubros compra*.
  5. `avg_basket_size` — `units_total / frequency` → captura *cómo es la canasta típica*.
  6. `recency_days` — días desde la última compra → captura *qué tan activo está hoy*.
- **Pipeline Spark:**
  `VectorAssembler` → `StandardScaler(withMean=True, withStd=True)` → `KMeans`.
- **Selección de k:** corremos K-Means para `k ∈ {3, 4, 5, 6}` sobre una **muestra del 10 %** de los clientes y comparamos por **silhouette score** (`pyspark.ml.evaluation.ClusteringEvaluator`). El k ganador se reentrena sobre los 131.186 clientes completos.
  - Silhouette mide para cada punto: qué tan cerca está de los puntos de su propio cluster vs. qué tan cerca está de los del cluster más cercano. Va de –1 (mal) a 1 (excelente). Lo que reportamos suele caer en 0.47–0.51, que para datos transaccionales reales es muy razonable.
- **Re-numeración por tamaño:** después de entrenar, renombramos los `cluster_id` para que **el cluster 0 sea siempre el mayoritario, el 1 el segundo, etc.** Esto estabiliza la interfaz: al re-entrenar, las etiquetas no se barajan al azar.
- **Salida (Gold):**
  - `cluster_assignments` (customer_id, cluster_id) — para uso en el dashboard.
  - `cluster_profiles` — un registro por cluster con promedio y mediana de cada feature → permite hablar de los segmentos en lenguaje de negocio.
  - `kmeans_search` — la búsqueda de k, para mostrar en el dashboard cómo se eligió.
  - El modelo `pyspark.ml` se persiste en `data/models/kmeans_*` para reuso.

### Si el profe pregunta "¿qué encontraron?"

Cinco segmentos típicos (los nombres y % varían por run; ver el dashboard en vivo):

| Cluster | Etiqueta de negocio | Cómo se distingue |
|---|---|---|
| 0 | 🟡 Ocasionales recientes | Baja frecuencia, canasta chica, recencia ≤ 30 días |
| 1 | 🟢 Regulares activos | Frecuencia media (~14 txns), canasta media, recencia baja |
| 2 | ⚫ Inactivos / dormidos | Frecuencia baja, recencia > 120 días |
| 3 | 🔵 VIPs / power users | Frecuencia alta (>30 txns), alta diversidad, recencia mínima |
| 4 | 🟠 Canasta grande | Baja frecuencia pero canasta media > 20 ítems |

**Validación cualitativa:** el cliente outlier 336296 (535 transacciones) cae correctamente en el cluster VIP.

---

## 2) La estrategia utilizada — este es el bloque crítico

> Esta sección es la que el otro grupo dijo que es la **más importante**. Hay que defenderla bien, no recitarla.

La estrategia es una cadena de decisiones de diseño concatenadas. Cada una resuelve un problema concreto del enunciado.

### 2.1 Por qué procesamiento distribuido (PySpark) y no pandas

- Cada canasta viene como una sola fila con la lista de productos pegada con espacios.
- Al hacer `explode` para tener `(transacción, producto)`, **pasamos de 1.108.987 filas a 10.591.792** — un orden de magnitud más.
- Pandas en memoria local soportaría esto, pero **el curso es de procesamiento distribuido** y la decisión es de futuro: si añaden 2 meses más, ya no entra. Spark escala a cluster sin reescribir el código de negocio.

### 2.2 Por qué medallion (Bronze / Silver / Gold)

- **Bronze = fidelidad a la fuente.** No transformamos nada de negocio. Si el profe nos pide ver el dato original, está ahí, en Parquet, particionado por `store_id`.
- **Silver = limpio y unido.** Aquí pasa el `explode`, se construye el `transaction_id` y se hace el join con el catálogo. Es la tabla "verdad operativa".
- **Gold = data marts para consumo.** Pre-agregados por dimensión (cliente, producto, categoría, día). El dashboard nunca lee Silver directamente para los KPIs — siempre Gold.
- **¿Para qué sirve esto?** Si mañana cambiamos cómo se calcula un KPI, sólo rehacemos Gold; Silver no se toca. Si cambia la fuente, sólo Bronze.

### 2.3 Por qué Parquet y no, por ejemplo, una base de datos

- Parquet es **columnar y comprimido**: las consultas analíticas leen sólo las columnas que necesitan.
- Es **particionable**: `store_id` se vuelve un directorio y filtrar por tienda no escanea las demás.
- No requiere un servicio corriendo (Postgres, MySQL): es portable, vive en el sistema de archivos.
- **DuckDB sobre Parquet** le da al dashboard la sensación de "base SQL" con latencia <100 ms, sin levantar Spark.

### 2.4 Decisiones puntuales que son nuestras (defendibles)

| Decisión | Por qué |
|---|---|
| `transaction_id = sha256(date|store|cust|product_list)` | El dataset no trae ID. Necesitamos uno **determinístico** para que re-procesar el mismo archivo no duplique. El hash sobre la canasta cumple. |
| `qty = count(*)` por `(transaction_id, product_id)` | El producto puede aparecer N veces en la lista de una canasta; interpretarlo como N unidades es la única lectura coherente con "no hay precios pero hay volumen". |
| `category = min(category_id) por product_id` | El catálogo tiene productos asignados a varias categorías. Joinear 1:N infla el conteo. Tomar el id menor es **determinístico** y no cambia las métricas relativas. |
| `StandardScaler` antes de K-Means | Las 6 features tienen rangos muy distintos. Sin escalar el algoritmo se sesga a una dimensión. |
| Re-numeración de clusters por tamaño | Estabiliza la UI: cluster 0 = mayoritario siempre. |
| FP-Growth sobre **top-200 productos**, `min_support = 0.05` | Con 449 productos y 1.1M canastas, `min_support = 0.01` revienta la JVM (OOM reproducible). Top-200 cubre el 99 % del volumen; subimos el soporte para mantener reglas accionables. |
| ALS con `coldStartStrategy='drop'` | Para no devolver NaNs cuando un cliente o producto no estaba en el entrenamiento. Documentado como limitación; fallback futuro = recomendar el top del cluster K-Means del cliente. |
| Reprocesar **todo** en cada ingesta nueva | El dataset cabe en memoria local (3 min de pipeline). Mantener delta-merge por particiones añade complejidad sin ganar nada todavía. Decisión deliberada de no sobre-ingeniería. |
| Manifest de `sha256` por archivo | Forma sencilla de detectar archivos nuevos o **modificados** (no sólo nuevos). Es el "estado" del lakehouse. |

### 2.5 La idempotencia (el profe seguro pregunta)

> *"¿Qué pasa si cargo dos veces el mismo archivo?"*

- **Bronze:** `mode("overwrite")` y partición por `store_id` → al sobrescribir queda en estado limpio.
- **Silver:** `transaction_id` es un hash determinístico → si la misma canasta entra dos veces, queda una sola tras el groupBy.
- **Gold:** se rehace desde Silver → no acumula nada.
- **Manifest:** detecta que el archivo no cambió y omite el reproceso completo.

Resultado: re-ejecutar el pipeline sobre el mismo input produce **bit a bit el mismo Gold**.

---

## 3) Diagrama de deployment

> Mostrar el diagrama del PDF (`docs/DIAGRAMA ARQUITECTURA.pdf`) en pantalla. El equivalente Mermaid está en `docs/arquitectura.md` sección 4.2.

### Qué hay que saber explicar (de izquierda a derecha)

```
┌─────────────────────────── Laptop / Workstation ────────────────────────────┐
│                                                                              │
│   Usuario / Profesor                                                         │
│         │ http://localhost:8501                                              │
│         ▼                                                                    │
│  ┌──────────────┐         ┌───────────────────────────────────────────────┐  │
│  │  Streamlit   │ ──SQL──▶│  DuckDB (in-process)                          │  │
│  │  (Python)    │         │   read_parquet('data/gold/**/*.parquet')      │  │
│  └──────┬───────┘         └───────────────────────────────────────────────┘  │
│         │ subprocess                                                         │
│         ▼                                                                    │
│  ┌──────────────────────────────────────────┐                                │
│  │  CLI:  python -m src.pipeline.ingest     │                                │
│  └──────┬───────────────────────────────────┘                                │
│         │                                                                    │
│         ▼                                                                    │
│  ┌───────────────────────────────────────────────────────────────────────┐   │
│  │  PySpark 3.5 — modo local[*]   (driver memory 8g)                     │   │
│  │   ├─ bronze.py  (lee CSV → Parquet raw)                                │   │
│  │   ├─ silver.py  (explode + join catálogo)                              │   │
│  │   ├─ gold.py    (data marts agregadas)                                 │   │
│  │   └─ models.py  (KMeans + FP-Growth + ALS, Spark MLlib)                │   │
│  └──────┬────────────────────────────────────────────────────────────────┘   │
│         │ R/W                                                                │
│         ▼                                                                    │
│  ┌────────────────────────── Sistema de archivos ─────────────────────────┐  │
│  │  data/landing/{Transactions,Products}/   ← entrada (drop zone)        │  │
│  │  data/landing/_manifest.json             ← estado (sha256 por archivo) │  │
│  │  data/landing/_runs.jsonl                ← log de corridas             │  │
│  │  data/bronze/                            ← Parquet raw                 │  │
│  │  data/silver/transactions_items/         ← Parquet limpio + explode    │  │
│  │  data/gold/                              ← 10 data marts               │  │
│  │  data/models/                            ← K-Means, FP-Growth, ALS     │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Puntos a recalcar al mostrarlo

- **Todo corre en un solo host** (laptop). Modo `local[*]` de Spark = un proceso JVM con tantos workers como cores.
- **Una sola JVM** se levanta por etapa del pipeline. No hay cluster manager separado, no hay HDFS — el sistema de archivos local sirve como "data lake".
- **El dashboard NO levanta Spark.** Lee Parquet con DuckDB. Esto es importante porque Spark tarda ~5 s en arrancar; DuckDB es instantáneo. Por eso el dashboard se siente fluido.
- **El subprocess** desde Streamlit al CLI de ingesta es la única "frontera" entre componentes; le pasamos `cwd=ROOT` y el python del venv para que la invocación sea limpia.
- **Promoción a producción** (si el profe pregunta): el mismo código corre en un cluster YARN/Kubernetes cambiando una sola línea de `master("local[*]")` a `master("yarn")`. Los Parquet pueden vivir en S3/GCS. El dashboard se containeriza con Docker.

---

## 4) Demo: cargar un dataset nuevo

> Esto es lo que el profe quiere ver con sus propios ojos.

### Checklist previa (hacer 15 min antes de la sustentación)

```bash
cd /Users/santiagoespinosa/Documents/dataProcess/semana13/proyecto

# 1. Estado del manifest debería estar consistente — si dice "needs_run": false, vamos bien
.venv/bin/python -m src.pipeline.ingest --check

# 2. Arrancar el dashboard
make app
#   se abre en http://localhost:8501
```

Dejar la página de Streamlit lista en la sección **"Generación de nuevos resultados"**.

### Script de la demo (≈ 4 min)

1. **Mostrar el estado inicial** (30 s)
   - En el dashboard, sección "Resumen Ejecutivo": señalar el KPI de transacciones (`1.108.986`) y de clientes (`131.186`). Esto es la "foto antes de cargar el archivo".

2. **Subir el archivo del profe** (30 s)
   - Pasar a "Generación de nuevos resultados".
   - Si es un archivo de transacciones: arrastrarlo al uploader "Archivo de transacciones".
   - Confirmar que aparece: *"Guardado en `data/landing/Transactions/XXX_Tran.csv`"*.

3. **Comprobar el cambio detectado** (30 s)
   - Click en "🔍 Comprobar cambios".
   - En el log debería aparecer:
     ```json
     {
       "new": ["Transactions/XXX_Tran.csv"],
       "needs_run": true
     }
     ```

4. **Ejecutar el pipeline incremental** (≈ 2.5 min)
   - Click en "▶️ Ejecutar si hay cambios".
   - El spinner se queda activo y el log va mostrando, por orden:
     - `=== ingest: running bronze ===` (~ 7 s)
     - `=== ingest: running silver ===` (~ 10 s)
     - `=== ingest: running gold ===` (~ 20 s)
     - `=== ingest: running models ===` (~ 90 s) — aquí podemos comentar: "se está reentrenando K-Means con los datos del nuevo archivo incluidos, lo mismo para FP-Growth y ALS".

5. **Validar el resultado** (30 s)
   - Volver a "Resumen Ejecutivo".
   - Mostrar que los KPIs cambiaron (los números suben con la nueva tienda).
   - Si la nueva tienda tiene un `store_id` distinto, aparece en el sidebar de filtros automáticamente. Lo seleccionamos para mostrar las gráficas filtradas.
   - Pasar a "Segmentación de Clientes" → mostrar que los clusters se recalcularon (en `kmeans_search` puede aparecer otro k ganador, lo cual es **esperado** y deseable: el modelo se adapta a los datos nuevos).

### Plan B — si el profe NO trae archivo

Hay uno listo en `docs/demo_assets/888_Tran.csv` (tienda 888, julio 2013, 3.010 canastas sintéticas). Arrastrarlo en el uploader y seguir el mismo guion.

### Qué decir si algo falla en la demo

- **Si el dashboard se ve "atascado":** el spinner sigue activo, **es normal** — la fase `models` toma ~90 s.
- **Si el upload falla por tamaño:** Streamlit por defecto permite hasta 200 MB. Para archivos más grandes, copiar el archivo manualmente a `data/landing/Transactions/` desde la terminal y luego usar "🔍 Comprobar cambios" → el manifest lo detecta igual.
- **Si el subprocess da error:** alternar a la terminal y ejecutar `.venv/bin/python -m src.pipeline.ingest --run`. El efecto es idéntico.

---

## 5) Banco de preguntas (organizado por tema)

> Esta sección es para estudiar antes. Está organizada por bloque para repasar focalizado.

### 5.1 K-Means a fondo

**P: ¿Por qué K-Means y no DBSCAN / jerárquico?**
R: K-Means escala bien y es el algoritmo "estándar" en MLlib. DBSCAN requiere elegir `eps` y `minPts`, no escala bien en alta dimensionalidad y no está en `pyspark.ml`. El clustering jerárquico es O(n²) en memoria y no es viable con 131k clientes. K-Means con silhouette para elegir k es la decisión más defendible para el alcance del curso.

**P: ¿Qué diferencia hay entre K-Means clásico, k-means++ y k-means||?**
R:
- *Clásico:* siembra los k centroides al azar uniformemente. Frágil — puede caer en óptimos locales malos.
- *k-means++:* siembra el primer centroide al azar y los siguientes con probabilidad proporcional a la distancia al cuadrado a los ya elegidos. Mucho más estable.
- *k-means|| ("scalable k-means"):* la variante paralelizable de k-means++ que usa Spark MLlib. Hace varias rondas de muestreo paralelo en lugar de elegir un punto a la vez. Igual de bueno que k-means++ pero distribuible.

**P: ¿Qué es exactamente el silhouette score?**
R: Para cada punto i:
- `a(i)` = distancia media a los puntos de su propio cluster.
- `b(i)` = distancia media al cluster vecino más cercano.
- `s(i) = (b - a) / max(a, b)` → va de –1 a 1.
El score global es el promedio de los `s(i)`. Cerca de 1 = clusters compactos y bien separados; cerca de 0 = clusters solapados; negativo = puntos probablemente mal asignados. Nuestros valores (0.47–0.51) son normales para datos transaccionales reales con clusters semánticamente reales pero geométricamente solapados.

**P: ¿Por qué no usaron el método del codo (elbow method)?**
R: Lo intentamos. El problema es que WCSS (within-cluster sum of squares) es monotónicamente decreciente con k, así que el "codo" es subjetivo — distintos observadores marcan k distintos. Silhouette devuelve un número absoluto que se puede ordenar, lo cual hace la selección reproducible y defendible.

**P: ¿Por qué la búsqueda de k la hacen sobre una muestra del 10%?**
R: Porque la búsqueda de k requiere entrenar K-Means múltiples veces (una por cada k candidato) y evaluar silhouette, que es O(n²) en el cluster cluster — calcular distancia media de cada punto a todos los del cluster vecino. Sobre los 131k clientes sería caro. Con una muestra del 10% (≈13k) la búsqueda toma segundos y el silhouette es estadísticamente representativo. El **k ganador** se reentrena sobre los **131k completos** para la asignación final.

**P: ¿Por qué silhouette varía entre corridas? La primera vez les dio k=5 y la última k=3.**
R: Porque la búsqueda usa una **muestra aleatoria del 10%** (seed=42, pero seed afecta solo la selección de la muestra, no garantiza independencia entre subconjuntos de features). Cada muestra puede privilegiar levemente un k distinto cuando los silhouettes están cerca (todos en ~0.47–0.51). Es esperado y forma parte de la naturaleza del problema. Soluciones más robustas: promedio sobre múltiples muestras (bootstrapping), o usar gap statistic en lugar de silhouette.

**P: ¿Por qué normalizan con StandardScaler y no con MinMaxScaler?**
R: StandardScaler convierte cada feature a media 0 y desviación 1 (z-score). Es preferible cuando las features pueden tener **outliers extremos** — como `frequency` con un cliente de 535 transacciones. MinMaxScaler `(x - min)/(max - min)` aplastaría todo el resto de los clientes contra el 0 por culpa de ese outlier. StandardScaler degrada de forma más elegante.

**P: ¿Cómo deciden que k=3, 4 o 5 tiene sentido de negocio si geométricamente los silhouettes son parecidos?**
R: Por **interpretabilidad**. El equipo de marketing tiene que poder mirar el cluster y decir "ah, esos son los VIPs". Cuando ejecutamos con k=5 obtuvimos cinco grupos con perfiles claramente distintos (ver `cluster_profiles`). Con k=3 el cluster "Regulares activos" y el "VIP" tienden a fusionarse, perdiendo la oportunidad de tratamiento diferenciado. **En sustentación si nos pregunta, decimos: silhouette es nuestro criterio matemático pero la decisión final debería validarse con el equipo de negocio sobre la interpretabilidad de los segmentos.**

**P: ¿Qué pasaría si tuvieran datos categóricos como género o ciudad?**
R: K-Means asume distancia euclidiana en variables numéricas. Para mezcla numérica + categórica habría que usar **k-prototypes** (Huang, 1998) o codificar las categóricas con one-hot y pasarlas por K-Means con cuidado (eso introduce ortogonalidad artificial entre categorías).

**P: ¿Las features que eligieron están correlacionadas, eso no contamina K-Means?**
R: Sí, hay correlación: `frequency` ↔ `units_total` r=0.84. En el heatmap se ve. **K-Means es robusto** a esto porque las features correlacionadas simplemente pesan más en la distancia, pero no rompen el algoritmo. Si quisiéramos descorrelacionar podríamos pasar por PCA antes (lo dejamos como mejora futura). Lo discutimos y consideramos que mantener las features originales es más interpretable para el negocio.

---

### 5.2 FP-Growth y reglas de asociación

**P: ¿Qué es FP-Growth y por qué no Apriori?**
R: FP-Growth (Frequent Pattern Growth, Han 2000) es un algoritmo de minería de itemsets frecuentes que evita la generación explícita de candidatos de Apriori. Construye un **FP-tree** (árbol prefijo compacto de las transacciones) y mina patrones recursivamente sobre ese árbol. Es típicamente **10–100× más rápido que Apriori** en datasets densos y, sobre todo, está distribuido en `pyspark.ml.fpm`. Apriori en Spark sólo está como librería externa.

**P: ¿Qué son support, confidence y lift?**
R:
- **Support(X)** = `P(X)` = fracción de canastas que contienen el itemset X. Es la "popularidad" del patrón.
- **Confidence(X → Y)** = `P(Y | X) = support(X∪Y) / support(X)`. Es "si compraron X, qué probabilidad hay de que también compren Y". Va de 0 a 1.
- **Lift(X → Y)** = `confidence(X→Y) / support(Y) = P(Y|X) / P(Y)`. Va de 0 a ∞. **Lift > 1** = X e Y se asocian más de lo que cabría esperar por azar. **Lift = 1** = independientes. **Lift < 1** = excluyentes.

**P: ¿Por qué `min_support = 0.05` y no `0.01`?**
R: Con 1.1M canastas y 449 productos, `min_support = 0.01` (11k canastas como umbral) hace que el FP-tree crezca exponencialmente — la JVM se queda sin memoria (OOM reproducible). Subimos a 0.05 (~55k canastas), lo cual sigue capturando los patrones de productos básicos. El filtro adicional de "top-200 productos por volumen" reduce el espacio de búsqueda a productos que sí pueden generar reglas a ese umbral.

**P: ¿Cómo interpretan que Prod 1 → Prod 2 tiene lift 4.4?**
R: Significa que ver Prod 1 en una canasta multiplica por 4.4 la probabilidad de que Prod 2 también esté ahí, comparado con el azar. Es una señal fuerte de **complementariedad** — productos que se consumen juntos. Aplicación directa: ponerlos cerca en góndola o sugerirlos juntos en e-commerce ("frequently bought together").

**P: ¿Por qué algunas reglas top tienen lift altos pero confianza moderada (~0.4)?**
R: Porque el lift está normalizado por el support del consecuente. Una regla `A → B` con confianza 0.4 pero `support(B) = 0.05` da lift 8. La confianza dice "cuán seguido pasa B dado A"; el lift dice "cuánto más seguido pasa de lo esperado". Para recomendación, **priorizamos lift** porque queremos sorprender al cliente con asociaciones no obvias, no sólo confirmar las populares.

**P: ¿Pueden tener reglas circulares A→B y B→A?**
R: Sí, FP-Growth las genera. En nuestra tabla vemos `Prod 1 → Prod 2 (conf 0.53)` y `Prod 2 → Prod 1 (conf 0.42)`. Lift es el mismo en ambas direcciones (la fórmula es simétrica), pero la confianza no. La asimetría refleja que P(2|1) ≠ P(1|2) cuando P(1) ≠ P(2).

**P: ¿Cómo evaluarían la calidad de las reglas en producción?**
R: Métricas de soporte/confianza/lift son sólo proxies estadísticos. La validación real es **A/B test en góndola o en banner web**: tomamos un subconjunto de clientes, mostramos recomendaciones generadas por la regla, medimos uplift en tasa de compra del consecuente. Eso queda fuera del alcance académico pero está documentado en el informe técnico como próximo paso.

---

### 5.3 ALS y filtrado colaborativo

**P: ¿Cómo funciona ALS por dentro?**
R: ALS (Alternating Least Squares) factoriza la matriz cliente×producto R en dos matrices de menor dimensión: U (clientes × factores latentes) y V (productos × factores latentes), de modo que `R ≈ U·Vᵀ`. El truco es **alternar**: fijar V y resolver U como un problema de mínimos cuadrados (cerrado en forma matricial), luego fijar U y resolver V. Se alterna hasta convergencia. Es paralelizable porque cada fila de U se resuelve independientemente dada V (y viceversa).

**P: ¿Qué son los "factores latentes"?**
R: Dimensiones abstractas que el modelo descubre y que no son interpretables directamente. Podrían capturar cosas como "preferencia por frescos", "sensibilidad al precio", "interés por orgánicos" — pero el modelo no nos dice qué representa cada factor. Es una representación densa del comportamiento. Configuramos `rank=16` factores: suficientes para capturar variabilidad sin sobreajustar.

**P: ¿Qué hace `implicitPrefs=True`?**
R: En ALS explícito (Netflix) tienes ratings de 1–5: el cliente dijo abiertamente "esto me gustó". En implícito tienes señales binarias o de conteo: el cliente compró X N veces — eso es una señal de **interés positivo**, no de preferencia ordenada. El modelo de Hu, Koren & Volinsky (2008) replantea el problema: en lugar de predecir el rating, predice un *binario* "interés sí/no", ponderado por una *confianza* que crece con el conteo. Por eso usamos `qty` como rating: a más compras del producto, más confianza.

**P: ¿Cómo manejan cold start?**
R: Configuramos `coldStartStrategy='drop'`: para clientes o productos que no estaban en el entrenamiento, ALS devolvería NaN. Con esa configuración los descarta. Como **fallback documentado** (no implementado por alcance): para clientes nuevos recomendamos el top-N global; para clientes con poco historial, recomendamos el top-N del cluster K-Means al que pertenecen. Es una integración natural de los dos modelos.

**P: ¿Cómo evaluarían ALS sin precisión@k ni ratings reales?**
R: Sin labels explícitos podemos usar:
- *Hit ratio* en hold-out temporal: dividir los datos por fecha, entrenar con los primeros 5 meses, medir cuántas de las recomendaciones generadas para cada cliente aparecen en sus compras del mes 6.
- *Diversidad intra-recomendación* (¿el top-10 cubre varias categorías o repite frescos?).
- *Cobertura* (¿qué % del catálogo aparece al menos una vez en las recomendaciones?).
No las implementamos por alcance académico pero están bien documentadas en la literatura.

**P: ¿Por qué `regParam=0.05`?**
R: Es el parámetro de regularización L2 que penaliza factores grandes — controla overfitting. 0.05 es el valor por defecto en Spark MLlib y suele ser razonable para datasets de este tamaño. En un entorno productivo correríamos cross-validation con `regParam ∈ {0.01, 0.05, 0.1, 0.5}` para refinar.

**P: ¿Y `rank=16`?**
R: 16 factores latentes. Heurística: típicamente entre 10 y 100. Cuanto más rank, más capacidad de modelar pero más memoria y más riesgo de overfitting. 16 es un compromiso conservador para 131k × 449 (matriz densa de 4M celdas con qty); con datos más grandes subiríamos.

---

### 5.4 Spark y procesamiento distribuido

**P: ¿Qué es un DAG en Spark?**
R: Directed Acyclic Graph — el plan lógico de transformaciones que Spark construye antes de ejecutar. Las **transformaciones** (`select`, `join`, `groupBy`, etc.) son *lazy* — sólo se ejecutan cuando llamas una **acción** (`count`, `write`, `collect`). Esto permite que Spark optimice el DAG entero (reordenar joins, podar columnas no usadas) antes de tocar datos. Es lo que hace al optimizador *Catalyst*.

**P: ¿Por qué configuran `spark.sql.shuffle.partitions = 8`?**
R: Por defecto Spark usa 200 particiones para shuffles, lo cual es ideal en cluster pero excesivo en `local[*]` con 8 cores. Con 200 particiones generamos muchos archivos pequeños y overhead de planificación. Con 8 particiones, una por core, las tareas saturan los cores sin desperdicio.

**P: ¿Qué es un shuffle? ¿Cuándo ocurre?**
R: Un shuffle es la redistribución de datos entre particiones cuando una operación necesita que las filas con la misma clave terminen juntas. Ocurre en `groupBy`, `join`, `distinct`, `orderBy`, etc. Es la operación **más cara** de Spark porque implica escritura a disco y red. En nuestro pipeline hay shuffles en:
- Silver: `groupBy(transaction_id, product_id)` para agregar qty.
- Gold: todos los `groupBy` para los marts.
- Models: K-Means redistribuye por cluster_id; FP-Growth construye el FP-tree centralizado en el driver.

**P: ¿Por qué partitionBy(store_id) en Bronze y Silver?**
R: Porque casi todas las consultas del dashboard filtran por tienda. Particionar por `store_id` permite a Spark hacer **partition pruning**: si filtramos `store_id = 102`, sólo lee los archivos de esa carpeta. Sin particionar leería los 4 archivos y descartaría. Trade-off: tener muchas particiones pequeñas (skew) si los archivos son chiquitos, pero con 4 tiendas relativamente balanceadas el riesgo es bajo.

**P: ¿Cuándo usaron `cache()` o `persist()` y por qué?**
R: En `models.py`: cacheamos `feats` (el DataFrame ya escalado) porque vamos a iterar K-Means varias veces sobre los mismos datos (una por cada k candidato). Sin cache, cada `fit()` releería desde Parquet y reaplicaría VectorAssembler + StandardScaler. Es la optimización más impactante del módulo: lo acelera ~3×.

**P: ¿Qué pasa si el dataset no cabe en memoria del driver?**
R: Spark deriva al disco automáticamente (spill). El programa sigue funcionando pero más lento. Si vemos spill masivo en la UI, las soluciones son: subir `spark.driver.memory`, repartir más particiones, evitar `collect()` masivos o cambiar a modo cluster.

**P: ¿Por qué `master("local[*]")`?**
R: `local[*]` arranca Spark en una sola JVM con tantos hilos worker como cores tenga la máquina. Es ideal para desarrollo y para datasets que caben en memoria. Para producción cambiaríamos a `master("yarn")` o `master("k8s://...")` — el resto del código de negocio no se toca. **Esa portabilidad es exactamente la razón de usar Spark en lugar de pandas.**

**P: ¿Pueden mostrar la Spark UI?**
R: Mientras corre el pipeline, Spark expone una UI en `http://localhost:4040`. Pero nosotros la deshabilitamos en producción (`spark.ui.showConsoleProgress=false`) para no contaminar el log. Si la pregunta llega: la habilitamos quitando esa línea y reiniciando.

**P: ¿Usaron `broadcast` para algún join?**
R: Sí, en `models.run_fpgrowth`: `items.join(F.broadcast(top_products), ...)`. `top_products` tiene sólo 200 filas, así que en lugar de redistribuir los 10M de items por `product_id`, Spark replica esa tablita en todos los workers y hace un *map-side join* — sin shuffle. Es una optimización clásica para joins con una tabla "dimensional" chica contra una "fact" grande.

---

### 5.5 Arquitectura medallion y diseño de datos

**P: ¿Por qué medallion y no otro patrón (star schema, data vault)?**
R: Medallion es el patrón de lakehouse moderno (popularizado por Databricks) y es el más simple que cumple los requisitos: separa la **fidelidad de la fuente** (Bronze) de la **modelación de negocio** (Silver/Gold). Star schema y data vault son patrones de modelado dimensional para data warehouses tradicionales — más rígidos. Medallion es más flexible para datos semi-estructurados y para experimentación analítica.

**P: ¿Es realmente un lakehouse o sólo un data lake con Parquet?**
R: Es un lakehouse "ligero" — usamos Parquet sin formato transaccional (Delta Lake, Iceberg). Eso significa que no tenemos *ACID transactions* ni *time travel*. Para producción real escalaríamos a Delta Lake (un wrapper sobre Parquet que añade un log transaccional). En modo académico, Parquet puro es suficiente porque controlamos completamente las escrituras.

**P: ¿Por qué Parquet y no CSV en Bronze?**
R: CSV es texto plano: leerlo siempre requiere parseo de strings → casteos → 5× más lento que Parquet. Parquet es:
- Columnar: si quieres sólo 2 columnas, lees 2.
- Comprimido (Snappy por defecto): ~5× más pequeño que CSV.
- Schema-on-write: el tipo de cada columna está en el footer.
- Pushdown de filtros: `where date > '2013-04-01'` se aplica al leer, no después.

**P: ¿Qué pasa si el esquema del CSV cambia (una columna nueva)?**
R: En el código actual fallaría — el `StructType` es estricto. Es una limitación conocida. Las opciones son:
1. Mantener el esquema como contrato y rechazar archivos que no lo cumplan (lo que hace el "validador de esquema" descrito en arquitectura.md).
2. Usar `mergeSchema=True` y dejar que Bronze acumule columnas. Aplica si queremos evolución de esquema, pero complica el procesamiento downstream.
Elegimos la opción 1 por simplicidad.

**P: ¿Por qué `transaction_id` es un hash y no un UUID?**
R: Porque el UUID es aleatorio — re-ejecutar el pipeline cambiaría los IDs y rompería joins downstream. El hash sobre `(date, store, customer, product_list)` es **determinístico**: la misma canasta siempre da el mismo ID. Eso preserva la idempotencia, que es no negociable en un pipeline batch.

**P: ¿Si una canasta tiene 0 productos qué hacen?**
R: La filtramos en Silver con `filter(F.col("product_list_raw").isNotNull())` y el `split` sobre la lista vacía la elimina del explode. No llega a Gold.

**P: ¿Por qué dim_customer_features vive en Gold y no en Silver?**
R: Porque `dim_customer_features` es un **agregado pre-calculado** específicamente para el dashboard y los modelos. Silver es la "verdad operativa" tabla larga; Gold es el agregado consumible. Mantener el feature engineering en Gold permite cambiar las features sin re-procesar Silver.

---

### 5.6 Recomendador, segmentación y negocio

**P: ¿Cómo decidirían qué recomendar a un cliente: ALS o FP-Growth?**
R: Son complementarios y resuelven problemas distintos:
- **ALS** sirve para recomendaciones personalizadas en el sitio web / app — "para ti": basadas en el historial individual del cliente.
- **FP-Growth** sirve para *cross-selling en góndola* y *"frequently bought together"* en e-commerce — basado en la canasta actual, no en el cliente histórico.

Estrategia productiva: usar ALS para la página de inicio personalizada, FP-Growth para "complemento" cuando el cliente agrega un producto al carrito.

**P: ¿Qué harían si un cluster tiene comportamiento que no se entiende?**
R: Primero: revisar los promedios y medianas (no sólo la media — la mediana es robusta a outliers). Segundo: muestrear 20–30 clientes del cluster y mirar sus canastas en detalle (qué compraron, en qué tiendas, cuándo). Tercero: si el cluster es muy pequeño (<5%) puede ser ruido — re-correr con un k menor o aceptar que es un grupo marginal.

**P: ¿El cluster "VIPs / power users" puede mezclar gente legítima con cuentas empresariales?**
R: Sí — y es un riesgo de negocio. Como mencionamos del cliente 336296 (535 transacciones), es candidato a verificación. Una mejora natural sería: para el cluster VIP, calcular un *score de probabilidad de cuenta empresarial* basado en la distribución de horas/días de compra (las cuentas humanas tienen estacionalidad; las B2B son más uniformes). No lo implementamos.

**P: ¿Cómo deciden si una recomendación es "buena"?**
R: Sin label sólo tenemos métricas indirectas:
- *Plausibilidad*: ¿la recomendación tiene sentido a la luz del historial?
- *Diversidad*: ¿el top-10 cubre varias categorías?
- *Novedad*: ¿no son sólo los top-sellers que el cliente ya conoce?
- *Cobertura*: ¿qué fracción del catálogo aparece al menos una vez?

Validación real requeriría A/B test en producción midiendo lift en compras.

**P: ¿Hay sesgo de popularidad en sus recomendaciones?**
R: Sí, ALS implícito tiende a recomendar los productos populares (los productos del top-200 que vimos en `dim_product_features`). Es un sesgo conocido del filtrado colaborativo implícito. Mitigaciones: filtrar productos ya comprados antes de mostrar; aplicar *re-ranking* que penalice el top global. No lo implementamos en esta entrega.

---

### 5.7 Operación, ingesta y robustez

**P: ¿Cómo evitan duplicados al re-procesar?**
R: Manifest de `sha256` + `transaction_id` hashed + `mode("overwrite")` en todas las capas. Idempotente por construcción.

**P: ¿Cómo monitorean el pipeline?**
R: Cada corrida graba un JSON en `data/landing/_runs.jsonl` con timestamps de inicio/fin, qué archivos cambiaron y los tiempos por etapa. El dashboard lo muestra en una tabla en la página de "Generación de nuevos resultados".

**P: ¿Cuánto tarda el pipeline completo?**
R: ~3 min en la laptop de desarrollo (16 GB RAM, modo `local[*]`). Bronze 7 s, Silver 7 s, Gold 15 s, Models 90 s. El 50 % del tiempo es el reentrenamiento de los tres modelos.

**P: ¿Y si vienen 20 archivos nuevos a la vez?**
R: El manifest los detecta a todos en un solo escaneo. El pipeline se ejecuta una sola vez sobre el conjunto. No tiene problema porque Bronze lee todos los archivos con un solo `read.csv(glob)`.

**P: ¿Y si el archivo nuevo está corrupto o tiene esquema inválido?**
R: Spark lanzará una excepción al leer el CSV — el pipeline falla limpiamente. La idea documentada en `arquitectura.md` es agregar un **validador previo** que mueva los archivos inválidos a `data/landing/_quarantine/`. No lo implementamos por alcance, pero la lógica es trivial (validar el `head -1` con regex de columnas antes de invocar el pipeline).

**P: ¿Cómo escalan si llegan 10 GB de datos nuevos en lugar de 100 MB?**
R: En modo `local[*]` haría spill a disco y se ralentizaría pero seguiría funcionando. Para algo realmente grande la promoción es cluster Spark. **Lo importante:** el código de negocio no cambia. Sólo cambia la línea `master("local[*]")` por `master("yarn")` o `master("k8s://...")` y la ubicación de los Parquet (de filesystem local a S3/GCS).

**P: ¿Qué pasa si el dashboard cae mientras el pipeline está corriendo?**
R: Streamlit y el pipeline corren en procesos distintos. El subprocess de ingesta sigue corriendo aunque Streamlit muera. Cuando Streamlit reinicie, leerá la nueva Gold y mostrará el estado actualizado.

**P: ¿Tienen logs persistentes del pipeline?**
R: Los runs incrementales sí, en `data/landing/_runs.jsonl`. Los logs detallados de Spark se imprimen a stdout y no los persistimos en archivo. Para producción agregaríamos un handler de logging que escriba a `logs/`. Aplica a versión académica que no lo necesita.

**P: ¿Qué hacen si tienen que reentrenar sólo K-Means sin re-correr Bronze→Gold?**
R: `make models` o `python -m src.pipeline.run --step models`. Lee `dim_customer_features` desde Gold y reentrena sólo K-Means + FP-Growth + ALS. Útil si experimentamos con hiperparámetros.

**P: ¿Pueden hacer ingesta sin reentrenar modelos?**
R: Sí, con el flag `--skip-models` o el checkbox del dashboard "Omitir reentrenamiento de modelos". Útil cuando sólo queremos refrescar KPIs y no nos importa que K-Means quede "desfasado" un rato.

---

### 5.8 Streamlit, DuckDB y dashboard

**P: ¿Por qué DuckDB y no leer Parquet directo con pandas?**
R: DuckDB es un motor analítico OLAP **embedded** que ejecuta SQL sobre Parquet sin cargarlo entero en memoria — usa lectura columnar y filtros pushdown. Pandas tendría que leer el Parquet completo a un DataFrame antes de filtrar. Para tablas de 130k+ filas con joins, DuckDB es ~10× más rápido y consume menos RAM.

**P: ¿Y por qué no Spark también en el dashboard?**
R: Porque Spark tarda ~5 s en arrancar la SparkSession y consume cientos de MB. Para responder un click de filtro eso es inaceptable. DuckDB inicia en milisegundos. Spark se justifica para los **batchs pesados** del ETL, no para consultas interactivas.

**P: ¿Cómo manejan el caché del dashboard?**
R: Decoradores `@st.cache_resource` (para la conexión DuckDB) y `@st.cache_data` (para las queries). Cuando hacemos ingesta nueva, limpiamos ambos con `st.cache_data.clear()` y `st.cache_resource.clear()` para que las páginas reflejen los datos frescos. Sin eso, el dashboard seguiría mostrando la Gold vieja.

**P: ¿Streamlit es bueno para producción?**
R: Para internal tools y prototipos analíticos, sí — es lo que se usa. Para front-end de cara al cliente final probablemente migraríamos a React + FastAPI. Para el alcance del curso y el caso de uso (consumo por equipo de analítica) es perfectamente defendible.

**P: ¿Por qué el dashboard tiene tantas páginas y filtros?**
R: Porque el enunciado pide cubrir 3 secciones (Resumen, Visualizaciones, Análisis Avanzado), nosotros desagregamos esta última en 3 (Segmentación, Recomendador, Generación) para que cada una tenga foco. Los filtros globales (tienda, fecha) se aplican consistentemente para que comparaciones entre páginas tengan sentido.

---

### 5.9 Comparaciones con alternativas

**P: ¿Por qué no usaron Airflow / Prefect?**
R: Para una pipeline con 4 etapas que se invocan en orden, Airflow es over-engineering. El Makefile + un CLI Python cumplen el mismo rol con cero infraestructura. Si la solución creciera a 30 jobs con dependencias complejas y SLAs, Airflow tendría sentido. Por ahora no.

**P: ¿Por qué no notebooks (Jupyter)?**
R: Notebooks son para exploración interactiva, no para producción. No son fácilmente reproducibles, mezclan estado y código, y no se versionan bien. El enunciado explícitamente dice "no se puede quedar sólo en notebook" — por eso entregamos un sistema completo con CLI y dashboard.

**P: ¿Por qué no MongoDB / Postgres en lugar de Parquet?**
R: Para cargas analíticas (agregaciones sobre millones de filas), un columnar como Parquet es 5–10× más rápido que un OLTP relacional. Postgres optimiza lecturas por fila (transaccionales); nosotros queremos lecturas por columna. Además, Parquet no requiere un servicio levantado — el dataset vive en archivos planos.

**P: ¿Por qué no Pandas si caben los datos en memoria?**
R: Caben **ajustadas**. 10 millones de filas en Silver consumen ~600 MB en pandas. Una mala query y pandas spila a swap. Spark fragmenta automáticamente. Más importante: el curso es de **procesamiento distribuido** — pandas sería elegir el camino corto y no demostrar lo que el curso enseña. Si el dataset triplica, pandas se rompe; el código Spark sigue intacto.

**P: ¿Por qué no Power BI / Tableau para el dashboard?**
R: Son licenciados y atan al ecosistema del proveedor. Streamlit es open source, en Python (el mismo lenguaje del pipeline) y permite controlar el comportamiento al nivel que queramos. Para una entrega académica y para mantener todo el stack abierto, Streamlit es la elección natural.

---

### 5.10 Limitaciones honestas (admitirlas refleja madurez)

**P: ¿Cuáles son las limitaciones de su solución?**
R: Sabemos exactamente dónde está. Las admitimos:
1. **Sin train/test split formal:** los modelos se evalúan con métricas internas (silhouette, support/confidence/lift, score). En producción agregaríamos hold-out temporal.
2. **Sin precios:** todas las métricas son *relativas*. Si llegan precios, abren análisis de margen y elasticidad — fáciles de añadir al pipeline.
3. **FP-Growth limitado a top-200 productos:** restricción de memoria en local. En cluster bajaríamos `min_support`.
4. **ALS con cold start descartado:** clientes nuevos no reciben recomendaciones hasta que aparezcan en el siguiente reentrenamiento.
5. **Reproceso completo en lugar de delta merge por partición:** decisión deliberada porque el dataset cabe en memoria local; en producción evaluaríamos Delta Lake con MERGE.
6. **Sin validación de esquema en la ingesta:** documentado en arquitectura como "validador" pero no implementado; un CSV mal formado rompe el pipeline en Bronze. En producción no se entregaría así.
7. **Sin observabilidad robusta:** logs van a stdout, no a un sistema centralizado. Para producción agregaríamos structured logging + métricas a Prometheus/Grafana.

**Sólo decir esto cuando el profe pregunte; no abrir el tema solo.**

---

### 5.11 Conceptuales y "trampa"

**P: ¿Qué es batch vs streaming? ¿Por qué eligieron batch?**
R: Batch procesa datos acumulados periódicamente. Streaming procesa cada evento al llegar. Elegimos batch porque:
- El profe nos da archivos completos, no eventos.
- El análisis de comportamiento de cliente es naturalmente a nivel de día/semana, no de segundo.
- La complejidad operativa de streaming (Kafka, Spark Structured Streaming, exactly-once semantics) no agrega valor para el caso de uso.

Si el cliente quisiera detectar fraude o stockouts en tiempo real, ahí sí justificaría streaming.

**P: ¿Qué es la consistencia eventual? ¿Aplica aquí?**
R: Es el modelo de consistencia donde, tras una escritura, los lectores pueden ver el valor viejo por un tiempo antes de converger al nuevo. Aplica en bases distribuidas (Cassandra, Dynamo). En nuestro pipeline batch, el `mode("overwrite")` es atómico a nivel de partición y el dashboard refresca el caché tras cada ingesta — no hay ventana de inconsistencia visible para el usuario.

**P: ¿Pueden ejecutar el pipeline en paralelo (dos corridas simultáneas)?**
R: No deberían: dos `mode("overwrite")` simultáneos sobre la misma carpeta producirían un estado corrupto. Si se necesitara, habría que añadir un **lock** (un archivo lock o usar Delta Lake). En la práctica el manifest ayuda: si un proceso ve "needs_run: false", no entra.

**P: ¿Qué pasaría con datos personales / GDPR?**
R: El `customer_id` es ya un identificador anonimizado. Si llegaran datos PII reales (nombres, emails), tendríamos que:
- Cifrar Bronze en reposo.
- Pseudonimizar antes de Silver con un mapping en una tabla de control de acceso.
- Implementar derecho al olvido (DELETE por customer_id en todas las capas).
No es relevante para el alcance académico pero estamos al tanto del tema.

**P: ¿Si tuvieran que rehacer este proyecto desde cero qué cambiarían?**
R:
- Empezaría con **Delta Lake** desde el principio (no Parquet puro) — `MERGE` resuelve idempotencia y *time travel* da auditoría gratis.
- Agregaría **dbt** o **SQLMesh** para modelar Gold como modelos SQL versionados.
- Setup de **tests con pytest** sobre lógica de transformación y modelos.
- Hold-out temporal y métricas de evaluación de ALS desde la primera iteración.
- Validador de esquema en ingesta (lo dejamos documentado pero no implementado).

**P: ¿Cuánto código escribieron / qué hicieron ustedes vs herramientas?**
R: La lógica de negocio (qué computar, qué tablas crear, qué features, qué umbrales, qué etiquetas de cluster) es nuestra. Las herramientas (Spark MLlib, DuckDB, Streamlit) hacen el trabajo pesado de cálculo. **Mostrar el código** — `models.py` tiene 270 líneas, `ingest.py` 130 líneas, el dashboard ~700 líneas. Es código propio, no copy-paste.

---

### 5.12 Si la pregunta es "muéstrenme X en el código"

| Si preguntan por... | Abrir y mostrar |
|---|---|
| Cómo se hace el explode de canastas | `src/pipeline/silver.py` líneas 49–58 |
| Cómo se construye el `transaction_id` | `src/pipeline/silver.py` líneas 28–39 |
| Selección del k de K-Means | `src/pipeline/models.py` función `run_kmeans`, lines 80–105 |
| Filtro top-200 en FP-Growth | `src/pipeline/models.py` función `run_fpgrowth`, líneas 140–162 |
| ALS implicit | `src/pipeline/models.py` función `run_als`, líneas 215–250 |
| Detección de archivos nuevos | `src/pipeline/ingest.py` funciones `_scan` y `diff` |
| Idempotencia del manifest | `src/pipeline/ingest.py` función `ingest`, líneas 100–135 |
| Subprocess desde Streamlit | `app/streamlit_app.py` función `_run_subprocess` |

---

## Apertura y cierre

**Apertura (30 s):**
> "Buenos días. Soy Santiago, junto con Cristian construimos esta solución. En las dos entregas pasadas mostramos la arquitectura y el resumen ejecutivo. Hoy nos toca el análisis avanzado: segmentación con K-Means, recomendaciones con FP-Growth y ALS, y la incorporación automática de nuevos datos. Lo vamos a mostrar todo corriendo en vivo, terminando con una demo donde cargamos un archivo nuevo y vemos el pipeline reaccionar."

**Cierre (30 s):**
> "Recapitulando: tenemos una segmentación de cinco clusters que distingue claramente VIPs, regulares, ocasionales, inactivos y compradores de canasta grande; 327 reglas de asociación con lift superior a 4 entre verduras; recomendaciones personalizadas para los 131 mil clientes; y un mecanismo de ingesta incremental que detecta cambios por hash y reprocesa de forma idempotente. La solución cumple los ocho requerimientos funcionales del enunciado y los cinco no funcionales. Quedamos atentos a preguntas."
