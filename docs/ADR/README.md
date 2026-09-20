# Registros de decisiones de arquitectura (ADR)

Cada fichero documenta **una** decisión: el contexto en el que se tomó, la
alternativa elegida y lo que se renunció al elegirla. Son cortos a propósito.

| # | Decisión | Estado |
|---|---|---|
| [0001](0001-pyspark-sobre-pandas.md) | PySpark como motor de procesamiento | Aceptada |
| [0002](0002-arquitectura-medallion.md) | Arquitectura medallion Bronze/Silver/Gold | Aceptada |
| [0003](0003-duckdb-en-serving.md) | DuckDB como motor de consulta del dashboard | Aceptada |
| [0004](0004-reprocesado-completo.md) | Reprocesar todo en cada ingesta | Aceptada |
| [0005](0005-stack-de-ui.md) | ECharts nativo + paleta validada | Aceptada |
| [0006](0006-computo-y-serving-separados.md) | Separar cómputo (CI) de serving (app) | Aceptada |
| [0007](0007-antecedentes-como-conjunto.md) | Conservar el antecedente de FP-Growth como conjunto | Aceptada |
