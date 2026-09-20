# 0007 — El antecedente de FP-Growth se conserva como conjunto

**Estado:** aceptada · **Fecha:** 2026-09 · **Corrige un error de la v1**

## Contexto

FP-Growth devuelve `antecedent` y `consequent` como arrays. El consecuente es
siempre unitario en MLlib; **el antecedente no**.

La primera versión hacía:

```python
.withColumn("antecedent_product_id", F.explode("antecedent"))
```

Eso convierte la regla `{3, 16} -> {21}` con confianza 0,575 en dos filas
—`3 -> 21` y `16 -> 21`, ambas con 0,575— que son **falsas** como reglas
individuales: esa confianza pertenece a la pareja, no a cada producto suelto.

Medido sobre el dataset real:

- 327 filas para sólo **215 pares distintos**.
- El par `5 -> 10` aparecía **once veces con once confianzas distintas**
  (0,459 / 0,560 / 0,618 / 0,553 ...).
- Grupos con `(confianza, lift)` idénticos y antecedentes distintos
  (`[4,5] -> 10`), la firma inequívoca del problema.

El informe técnico reproducía el error sin notarlo: su tabla de top-10 listaba
"Prod 21 -> Prod 16" dos veces con cifras diferentes.

## Decisión

Persistir el antecedente íntegro:

| Columna | Contenido |
|---|---|
| `antecedent_ids` | array ordenado de productos |
| `antecedent_label` | `3 + 16`, legible |
| `antecedent_size` | cardinalidad, para poder filtrar |
| `support` | recuperado de `freqItemsets`; antes se perdía |

Las consultas usan `list_contains(antecedent_ids, :pid)` y la interfaz muestra
la regla completa (`3 + 16 -> 21`).

## Consecuencias

- **270 reglas reales** donde antes había 327 filas infladas.
- La interfaz ofrece un interruptor "sólo reglas simples" para quien quiera
  únicamente antecedentes de un ítem.
- El diagrama sankey usa **sólo** reglas de antecedente simple: un diagrama de
  flujo no puede representar honestamente una condición conjunta.
- `tests/test_product_rules.py` fija las invariantes —ninguna regla duplicada,
  ninguna confianza contradictoria, el consecuente nunca dentro del
  antecedente— para que no vuelva a colarse.
