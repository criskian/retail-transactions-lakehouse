# 0004 — Reprocesar todo el lakehouse en cada ingesta

**Estado:** aceptada · **Fecha:** 2026-06

## Contexto

Al llegar un fichero nuevo al landing hay dos caminos: calcular el delta y
actualizar sólo las particiones afectadas, o rehacer el lakehouse completo.

## Decisión

Rehacer todo: `bronze → silver → gold → models → export`. La detección de
cambios se limita a decidir **si** hay que correr, no **qué** correr; se hace
con un manifest de `sha256` por fichero.

## Consecuencias

- Una corrida completa tarda ~8 minutos sobre el dataset real. Aceptable para
  un proceso batch que se dispara cuando llegan datos.
- Los marts y los modelos son siempre consistentes entre sí por construcción.
  Con deltas hay que razonar sobre qué modelo se entrenó con qué versión.
- No hay lógica de merge incremental que mantener ni que depurar.
- Los modelos se reentrenan siempre, aunque el cambio sea pequeño. Como efecto
  secundario, el `k` de K-Means podía cambiar con muy pocos datos nuevos; se
  mitigó promediando el silhouette sobre varias submuestras.

## Cuándo habría que revisarlo

Cuando una corrida completa pase de ~30 minutos, o cuando haya que ingerir
varias veces al día. En ese punto conviene `MERGE` por partición de fecha sobre
un formato transaccional (Delta Lake o Iceberg), que además daría *time travel*.

## Garantías de idempotencia

1. `transaction_id` es un hash determinista de la canasta.
2. Las escrituras son atómicas (`storage.py`): se escribe a `.__tmp` y se
   renombra. Un fallo a mitad conserva la versión anterior en vez de dejar un
   directorio vacío.
3. El manifest sólo se actualiza si la corrida termina bien; si falla, la
   siguiente reintenta.
