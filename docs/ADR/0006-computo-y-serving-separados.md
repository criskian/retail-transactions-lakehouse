# 0006 — Separar el cómputo del serving

**Estado:** aceptada · **Fecha:** 2026-09

## Contexto

Para publicar una demo, el proyecto original tenía tres bloqueos:

1. El dashboard consultaba **Silver (379 MB)** en ocho sitios, lo cual además
   contradecía su propio documento de arquitectura.
2. PySpark pesa 317 MB y necesita una JVM. Las plataformas de despliegue
   gratuitas no tienen Java.
3. La página de ingesta lanzaba Spark como subproceso, imposible en un sistema
   de ficheros efímero.

## Decisión

```
 Desarrollo / CI                          Serving
 ---------------                          -------
 PySpark + JDK 17                         Streamlit + DuckDB
 bronze -> silver -> gold -> models  -->  serving.duckdb (24 MB)
 export_serving.py                        sin JVM, sin estado, read-only
```

- Se añadieron cuatro marts (`fact_product_daily`, `fact_category_daily`,
  `fact_customer_daily`, `dim_customer_top_products`) que replican, ya
  agregadas, las consultas que antes iban contra Silver.
- `requirements.txt` es **sólo** serving. El pipeline vive en
  `requirements-pipeline.txt`. Si PySpark se cuela en el primero, el despliegue
  revienta.
- El pipeline corre en GitHub Actions: los runners de repositorios públicos dan
  4 vCPU / 16 GB, minutos ilimitados y JDK Temurin precacheado.
- El artefacto se publica como asset de un release; la app lo descarga y lo
  cachea al arrancar.

## Consecuencias

| | Antes | Ahora |
|---|---|---|
| Datos que necesita la app | 379 MB | 24 MB |
| Dependencias de la app | PySpark + JVM | DuckDB |
| Desplegable en plataforma gratuita | No | Sí |

- La página de ingesta detecta el entorno: en local ejecuta el pipeline de
  verdad; desplegada explica el mecanismo y enlaza al workflow.
- Los datos de la demo son tan recientes como la última corrida de CI. Es un
  proceso batch, así que no es una limitación real.
