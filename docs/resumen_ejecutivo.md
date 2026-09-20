# Resumen Ejecutivo — Análisis de Transacciones de Supermercado

**Curso:** Procesamiento Distribuido de Datos · G1
**Autores:** Santiago Espinosa · Cristian Molina
**Entrega:** 29 de mayo de 2026
**Período analizado:** 2013-01-01 → 2013-06-30 (181 días, 4 tiendas)

> Este documento acompaña al dashboard interactivo (`make app`). Aquí se destilan los hallazgos numéricos y se proponen lecturas de negocio. Todas las métricas son **relativas** (unidades, transacciones, frecuencia, diversidad) porque el dataset no incluye precios.

---

## 1. Indicadores globales (KPIs)

| Indicador | Valor | Interpretación |
|---|---:|---|
| **Total de unidades vendidas** | **10.591.793** | ~58.500 unidades/día en promedio |
| **Número de transacciones** | **1.108.986** | ~6.127 canastas/día |
| **Clientes únicos** | **131.186** | base activa en el semestre |
| **Tamaño promedio de canasta** | **9,55 items** | una canasta típica trae ~10 productos |
| **Tiendas activas** | 4 | 102, 103, 107, 110 |
| **Productos transaccionados** | 449 (de **69.891** en el catálogo) | catálogo muy ancho, *long tail* concentrado |
| **Categorías observadas** | **20** (de 50 en el catálogo) | 60 % del catálogo no se movió en el semestre |

**Lectura ejecutiva:** la operación es de **alto volumen y canasta pequeña pero diversa** (10 items por compra). Una porción muy grande del catálogo permanece inactiva en el período, lo cual sugiere oportunidad de depuración o re-merchandising.

---

## 2. Distribución por tienda

| Tienda | Unidades | Transacciones | % del total |
|---|---:|---:|---:|
| **103** | 4.234.392 | 407.129 | **37%** |
| 102 | 2.562.327 | 314.286 | 28% |
| 107 | 2.410.968 | 254.633 | 23% |
| 110 | 1.384.106 | 132.938 | 12% |

**Hallazgo:** la tienda **103 mueve 3x más volumen que la 110**. Cualquier modelo agregado debe controlar por tienda para no enmascarar comportamientos locales.

---

## 3. Top productos y clientes

### Top 10 productos por unidades vendidas

Liderazgo muy concentrado en pocos IDs: los 5 productos más vendidos (5, 10, 3, 4, 6) están todos entre **254k y 300k unidades** en el semestre — diferencias chicas entre ellos, lo que indica que **no hay un solo "ganador" sino un núcleo de productos básicos** que rotan de forma similar.

| Producto | Unidades |
|---|---:|
| Prod 5 | 300.526 |
| Prod 10 | 290.313 |
| Prod 3 | 269.855 |
| Prod 4 | 260.418 |
| Prod 6 | 254.644 |

### Top 10 clientes por número de transacciones

Aquí el hallazgo es de outlier: el cliente **336296** registra **535 transacciones** en el semestre — el resto del top-10 está entre **129 y 163**. Es 3 a 4 veces más activo que el siguiente cliente. Candidato natural a:
- Verificación de identidad (¿es un solo cliente o una cuenta empresarial?)
- Programa de fidelización VIP si la cuenta es legítima

---

## 4. Estacionalidad y días pico

| Día | Transacciones | Comentario |
|---|---:|---|
| 🔺 **2013-06-15 (sáb)** | 9.476 | día pico del semestre |
| 2013-05-11 (sáb) | 8.854 | segundo más alto |
| 2013-02-03 (dom) | 8.523 | tercero más alto |
| 🔻 **2013-01-01 (mar)** | 2.860 | mínimo (año nuevo) |

**Patrón por día de la semana** (transacciones totales en el semestre):

| Día | Transacciones | Índice (Mié=100) |
|---|---:|---:|
| **Domingo** | 191.406 | 139 |
| **Sábado** | 189.015 | 138 |
| Jueves | 158.766 | 116 |
| Martes | 150.739 | 110 |
| Lunes | 142.445 | 104 |
| Viernes | 139.370 | 102 |
| Miércoles | 137.245 | 100 |

> Jueves y viernes aparecían intercambiados en la primera versión. El jueves es
> el tercer día más activo. Las cifras se generan con `docs/regen_figures.py`.

**Lectura ejecutiva:** la operación está **fuertemente sesgada al fin de semana** (sáb+dom ~39 % más volumen que un miércoles). Esto impacta directamente decisiones de staffing, abastecimiento e inventario.

---

## 5. Categorías más relevantes

| # | Categoría | Unidades | Transacciones | Clientes |
|---|---|---:|---:|---:|
| 1 | VERDURAS RAÍZ, TUBÉRCULO Y BULBOS | 1.811.523 | 735.056 | 107.027 |
| 2 | VERDURAS DE FRUTOS | 1.410.750 | 647.476 | 101.532 |
| 3 | JUGOS | 729.513 | 465.330 | 91.495 |
| 4 | AROMÁTICAS CONDIMENTOS | 493.388 | 400.893 | 82.198 |
| 5 | AROMÁTICAS MEDICINALES | 294.753 | 279.926 | 67.082 |

**Hallazgo:** el supermercado es fuertemente **fresco-vegetal**: las tres categorías top son alimentos frescos (verduras + jugos). El proxy de rentabilidad por volumen apunta al frescos como driver principal del negocio.

> **Caveat de calidad del catálogo:** 206 IDs de producto (46% de los transaccionados) no tienen mapeo a categoría. Aparecen como "(sin categoría)" en el dashboard y es la **barra más alta** del gráfico de categorías. Es un hallazgo importante: hay un problema de calidad de datos en el catálogo que se reportará al cliente.

---

## 6. Comportamiento de clientes

| Estadístico | Frecuencia (txns por cliente) |
|---|---:|
| Mediana | **4** |
| Promedio | 8,45 |
| Máximo (outlier) | 535 |

**Distribución por número de compras:**

| Segmento | Clientes | % |
|---|---:|---:|
| 1 sola compra | 34.513 | **26%** |
| 2 a 5 | 41.561 | 32% |
| 6 a 10 | 21.290 | 16% |
| 11 a 20 | 18.972 | 14% |
| más de 20 | 14.850 | 11% |

**Concentración (curva de Pareto):**

- Top 100 clientes → **1,0%** de las transacciones
- Top 1.000 clientes → **6,5%**
- Top 10% de clientes (13.118) → **41,9%**

**Lectura ejecutiva:**
- 1 de cada 4 clientes vino **una sola vez**. Es la oportunidad más grande de **retención**.
- El 10% más activo genera **42% del volumen** — patrón típico de Pareto. Justifica un **programa de fidelización dirigido**.

---

## 7. Relaciones entre variables (heatmap de correlación)

Correlación Pearson entre 6 features de cliente:

| | freq | units | prods | cats | basket | recency |
|---|---:|---:|---:|---:|---:|---:|
| frequency | 1.00 | 0.84 | 0.73 | 0.63 | 0.17 | **-0.44** |
| units_total | 0.84 | 1.00 | 0.85 | 0.69 | 0.46 | -0.39 |
| distinct_products | 0.73 | 0.85 | 1.00 | 0.69 | 0.58 | -0.46 |
| distinct_categories | 0.63 | 0.69 | 0.69 | 1.00 | 0.57 | -0.48 |
| avg_basket_size | 0.17 | 0.46 | 0.58 | 0.57 | 1.00 | -0.15 |
| recency_days | -0.44 | -0.39 | -0.46 | -0.48 | -0.15 | 1.00 |

**Tres lecturas clave:**

1. **Frecuencia y diversidad de productos correlacionan fuerte (0.73-0.85):** los clientes que vienen más también compran cosas más distintas. No es solo "vienen más y repiten lo mismo".
2. **Tamaño de canasta es relativamente independiente de la frecuencia (0.17):** un cliente puede ser frecuente con canastas pequeñas (compra diaria de frescos) o esporádico con canastas grandes (compra mensual). Esto sugiere **al menos dos segmentos diferenciables** para el K-Means de la próxima entrega.
3. **Recencia correlaciona negativamente con todo (-0.39 a -0.48):** los clientes recientes son los más activos. Es la señal esperada y valida que las features tienen contenido predictivo.

---

## 8. Conclusiones y aplicaciones empresariales

### Hallazgos principales

1. **Operación dominada por frescos y fines de semana.** La carga operacional se concentra en sáb+dom, con verduras y jugos como categorías ancla.
2. **Catálogo subutilizado.** El **99,4 %** de los SKUs catalogados no se vendieron en 6 meses; eso es una oportunidad de depuración.
3. **Calidad de datos del catálogo:** 46% de los productos transaccionados no tienen categoría asignada — pendiente de saneamiento.
4. **Base de clientes con cola larga:** 26% son one-shot. Hay un tercio del volumen capturable con políticas de retención básicas.
5. **Distribución Pareto clásica:** top 10% genera 42% del volumen.

### Aplicaciones empresariales inmediatas

| Decisión | Insumo del análisis |
|---|---|
| **Staffing y abastecimiento** | Estacionalidad semanal (sáb+dom ≈ 139 % de un miércoles) |
| **Programa de fidelización** | Top 10% de clientes = 42% del volumen |
| **Campañas de retención** | 34.513 clientes con 1 sola compra |
| **Saneamiento de catálogo** | 206 productos sin categoría, 69.442 SKUs nunca vendidos |
| **Detección de cuentas atípicas** | Cliente 336296 con 535 transacciones vs media de 8 |

### Lo que habilita esto para la próxima entrega

Las correlaciones del heatmap **validan que las features (frecuencia, volumen, diversidad, basket size, recencia) capturan dimensiones distintas del comportamiento del cliente**. La existencia de outliers en frecuencia y la baja correlación entre basket_size y frequency sugieren que el **K-Means encontrará al menos 3-4 segmentos diferenciables** (compradores ocasionales, recurrentes de basket chica, recurrentes de basket grande, VIP). Esto se va a presentar el **9-10 de junio**.

---

## 9. Cómo reproducir estos resultados

```bash
invoke install    # crea .venv e instala dependencias
invoke pipeline   # bronze -> silver -> gold -> models (~8 minutos)
invoke export     # construye data/serving/serving.duckdb
invoke app        # dashboard interactivo en http://localhost:8501
```

Las tablas Gold de las que se derivan estas conclusiones están en `data/gold/`:
`fact_kpis`, `fact_sales_daily`, `dim_customer_features`, `dim_product_features`, `fact_category_metrics`.
