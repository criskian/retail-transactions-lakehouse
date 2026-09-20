# 0002 — Arquitectura medallion Bronze / Silver / Gold

**Estado:** aceptada · **Fecha:** 2026-05

## Contexto

El dataset llega como CSV sin identificador de transacción, con un catálogo
producto→categoría que no es 1:1 y sin precios. Hacen falta varias capas de
transformación antes de poder responder una pregunta de negocio, y conviene
poder cambiar una sin rehacer las demás.

## Decisión

Tres capas materializadas en Parquet:

| Capa | Contenido | Regla |
|---|---|---|
| **Bronze** | Copia fiel del CSV, particionada por `store_id` | Sin lógica de negocio |
| **Silver** | `transactions_items` tras el explode y el join con el catálogo | La verdad operativa |
| **Gold** | Marts agregados por dimensión | Lo que consume la aplicación |

## Consecuencias

- Cambiar cómo se calcula un KPI sólo rehace Gold (~75 s), no Silver (~13 s más).
- Si la fuente cambia de formato, sólo se toca Bronze.
- Bronze conserva el dato original, así que cualquier discrepancia es auditable.
- Cuesta espacio: Silver ocupa 379 MB para 10,6 M de filas. Es el precio de no
  recalcular el explode en cada consulta.

## Notas

Dos decisiones de modelado que viven en Silver y conviene recordar:

- `transaction_id = sha2(date|store|customer|product_list)`. Determinista, así
  que reprocesar el mismo fichero no duplica nada. Un UUID habría roto la
  idempotencia.
- `category = min(category_id)` por producto. El catálogo asigna varios
  category_id al mismo producto; sin resolverlo a 1:1 el join infla los
  conteos. El criterio es arbitrario pero determinista, y no altera las
  métricas relativas.
