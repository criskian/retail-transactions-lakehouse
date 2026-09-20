# 0003 — DuckDB como motor de consulta del dashboard

**Estado:** aceptada · **Fecha:** 2026-05

## Contexto

El dashboard hace decenas de consultas agregadas por interacción. Levantar una
SparkSession para cada una es inviable: sólo arrancar la JVM tarda más que toda
la consulta.

## Decisión

Spark **calcula**; DuckDB **sirve**. La aplicación abre un único fichero DuckDB
en modo *read-only* y no importa PySpark en ningún momento.

## Consecuencias

Latencias medidas sobre el artefacto real (24,5 MB):

| Consulta | Tiempo |
|---|---|
| Historial de un cliente | 5 ms |
| Recomendaciones ALS de un cliente | 6 ms |
| Boxplot sobre 131.186 clientes | 60 ms |
| Top-10 productos filtrado por fecha y tienda | ~400 ms |

- El dashboard se siente instantáneo y no necesita JVM.
- Es lo que permite el despliegue descrito en
  [0006](0006-computo-y-serving-separados.md).
- Hay dos motores SQL en el proyecto, con dialectos parecidos pero no idénticos.
  Se acepta porque la frontera es nítida: nada de Gold hacia arriba usa Spark.

## Nota sobre el exportador

`export_serving.py` sólo empaqueta; no transforma. Todo cálculo sigue viviendo
en `gold.py` y `models.py` con PySpark. Materializa cada tabla con `ORDER BY`
por las columnas de filtrado habituales para mejorar los *zone maps*.
