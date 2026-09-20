# 0001 — PySpark como motor de procesamiento

**Estado:** aceptada · **Fecha:** 2026-05

## Contexto

Cada fila del dataset es una canasta con la lista de productos concatenada por
espacios. Para analizar por producto hay que explotarla a formato largo, y eso
lleva la tabla de **1.108.987 filas a 10.591.792**: un orden de magnitud más.

Pandas aguantaría ese volumen en una laptop con holgura. La pregunta real no era
si cabe hoy, sino qué pasa cuando el dataset crezca y quién mantiene el código.

## Decisión

PySpark 3.5 en modo `local[*]` para todo el ETL y el modelado.

## Consecuencias

**A favor**
- El mismo código corre en un cluster cambiando `master("local[*]")` por
  `master("yarn")`; la lógica de negocio no se toca.
- `groupBy`/`join` declarativos que Catalyst optimiza, con particionado nativo
  por `store_id`.
- MLlib mantiene el modelado dentro del mismo motor: no hay que reescribir el
  *feature engineering* en scikit-learn ni traer los datos al driver.

**En contra**
- Arrancar una JVM cuesta ~5 s por etapa, lo que hace tediosa la iteración.
- Trae tres incompatibilidades de entorno reales (Java 21, `python3` ausente en
  Windows, Hadoop native IO) que hubo que resolver en `spark_session.py`.
- La dependencia pesa 317 MB, razón por la que el serving se separó
  ([0006](0006-computo-y-serving-separados.md)).

## Alternativas descartadas

- **Pandas / Polars** — más rápido para este tamaño, pero obliga a reescribir
  todo si el volumen crece, y deja el proyecto sin respuesta a "¿y si mañana son
  500 millones de filas?".
- **DuckDB para el ETL** — excelente motor, pero monomáquina por diseño. Se usa
  donde sí es la herramienta correcta: la capa de consulta
  ([0003](0003-duckdb-en-serving.md)).
