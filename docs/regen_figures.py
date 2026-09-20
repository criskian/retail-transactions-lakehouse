"""Regenera todas las cifras que aparecen en los documentos.

Los informes de la primera entrega tenían números escritos a mano que se
desincronizaron del pipeline: el catálogo contaba mapeos en vez de productos,
jueves y viernes estaban intercambiados, y la lista de recomendaciones de un
cliente concreto no reproducía. Este script emite las cifras desde la capa de
serving para que copiarlas a mano deje de ser una fuente de error.

    python docs/regen_figures.py            # imprime un bloque Markdown
    python docs/regen_figures.py --json     # emite JSON, para automatizar
"""

from __future__ import annotations

import argparse
import contextlib
import json
import sys
from pathlib import Path

import duckdb

# La consola de Windows usa cp1252 y aborta al imprimir flechas o acentos.
for _s in (sys.stdout, sys.stderr):
    with contextlib.suppress(AttributeError, ValueError, OSError):
        _s.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parents[1]
SERVING = ROOT / "data" / "serving" / "serving.duckdb"
LANDING = ROOT / "data" / "landing"

DIAS = ["Domingo", "Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado"]


def _catalogo() -> dict:
    """Se lee del CSV crudo: el catálogo completo no llega a Gold."""
    pc = LANDING / "Products" / "ProductCategory.csv"
    cats = LANDING / "Products" / "Categories.csv"
    if not pc.exists():
        return {}
    con = duckdb.connect()
    productos = con.execute(
        f"""select count(distinct "v.Code_pr")
            from read_csv('{pc.as_posix()}', delim='|', header=true)"""
    ).fetchone()[0]
    mapeos = con.execute(
        f"""select count(*)
            from read_csv('{pc.as_posix()}', delim='|', header=true)"""
    ).fetchone()[0]
    n_cats = con.execute(
        f"""select count(*)
            from read_csv('{cats.as_posix()}', delim='|', header=false)"""
    ).fetchone()[0]
    con.close()
    return {
        "productos_en_catalogo": productos,
        "mapeos_producto_categoria": mapeos,
        "categorias_en_catalogo": n_cats,
    }


def collect() -> dict:
    if not SERVING.exists():
        sys.exit(f"No existe {SERVING}. Ejecuta `invoke pipeline && invoke export`.")

    con = duckdb.connect(str(SERVING), read_only=True)
    try:
        kpis = con.execute("select * from fact_kpis").df().iloc[0].to_dict()

        tiendas = con.execute(
            """select store_id, sum(units) units, sum(txn_count) txns
               from fact_sales_daily group by 1 order by units desc"""
        ).df()
        total_units = tiendas["units"].sum()

        dow = con.execute(
            """select dayofweek(date) dow, sum(txn_count) txns
               from fact_sales_daily group by 1 order by txns desc"""
        ).df()
        dow["dia"] = dow["dow"].apply(lambda i: DIAS[int(i)])
        miercoles = float(dow.loc[dow["dia"] == "Miércoles", "txns"].iloc[0])

        productos = con.execute(
            """select product_id, units_total from dim_product_features
               order by units_total desc limit 5"""
        ).df()

        clientes = con.execute(
            "select customer_id, frequency from dim_customer_features order by frequency desc limit 5"
        ).df()

        categorias = con.execute(
            """select coalesce(category_name, 'Sin categoría') categoria,
                      units, txn_count, customers
               from fact_category_metrics order by units desc limit 6"""
        ).df()

        sin_categoria = con.execute(
            "select count(*) from dim_product_features where category_id is null"
        ).fetchone()[0]

        perfiles = con.execute(
            """select cluster_id, n_customers,
                      round(avg_frequency, 1) frecuencia,
                      round(avg_units_total, 0) unidades,
                      round(avg_distinct_products, 0) productos,
                      round(avg_distinct_categories, 1) categorias,
                      round(avg_avg_basket_size, 1) canasta,
                      round(avg_recency_days, 0) recencia
               from cluster_profiles order by cluster_id"""
        ).df()

        busqueda_k = con.execute(
            """select k, round(silhouette_mean, 4) media,
                      round(silhouette_std, 4) desviacion, n_samples
               from kmeans_search order by k"""
        ).df()

        reglas = con.execute(
            """select antecedent_label, consequent_product_id,
                      round(support, 4) soporte, round(confidence, 4) confianza,
                      round(lift, 3) lift
               from product_rules order by lift desc, confidence desc limit 10"""
        ).df()

        reglas_stats = (
            con.execute(
                """select count(*) total,
                      count(*) filter (where antecedent_size = 1) simples
               from product_rules"""
            )
            .df()
            .iloc[0]
            .to_dict()
        )

        freq = (
            con.execute(
                """select median(frequency) mediana, avg(frequency) media,
                      max(frequency) maximo
               from dim_customer_features"""
            )
            .df()
            .iloc[0]
            .to_dict()
        )

        segmentos_compra = con.execute(
            """select case when frequency = 1 then '1 sola compra'
                           when frequency <= 5 then '2 a 5'
                           when frequency <= 10 then '6 a 10'
                           when frequency <= 20 then '11 a 20'
                           else 'más de 20' end segmento,
                      count(*) clientes
               from dim_customer_features group by 1 order by clientes desc"""
        ).df()

        total_txns = con.execute("select sum(frequency) from dim_customer_features").fetchone()[0]
        pareto = {}
        for n in (100, 1000, int(kpis["total_customers"] * 0.1)):
            s = con.execute(
                f"""select sum(frequency) from (
                        select frequency from dim_customer_features
                        order by frequency desc limit {n})"""
            ).fetchone()[0]
            pareto[f"top_{n}"] = round(100 * s / total_txns, 1)

        correlacion = (
            con.execute(
                """select frequency, units_total, distinct_products,
                      distinct_categories, avg_basket_size, recency_days
               from dim_customer_features"""
            )
            .df()
            .corr()
            .round(2)
        )

        pca_var = con.execute("select max(explained_variance) from cluster_pca").fetchone()[0]

        dia_pico = (
            con.execute(
                """select date, sum(txn_count) txns from fact_sales_daily
               group by 1 order by txns desc limit 1"""
            )
            .df()
            .iloc[0]
            .to_dict()
        )
        dia_valle = (
            con.execute(
                """select date, sum(txn_count) txns from fact_sales_daily
               group by 1 order by txns asc limit 1"""
            )
            .df()
            .iloc[0]
            .to_dict()
        )
    finally:
        con.close()

    tiendas["pct"] = (tiendas["units"] / total_units * 100).round(0)
    dow["indice"] = (dow["txns"] / miercoles * 100).round(0)
    perfiles["pct"] = (perfiles["n_customers"] / perfiles["n_customers"].sum() * 100).round(0)

    return {
        "kpis": {k: (str(v) if hasattr(v, "isoformat") else v) for k, v in kpis.items()},
        "catalogo": _catalogo(),
        "canasta_media": round(float(kpis["total_units"]) / float(kpis["total_transactions"]), 2),
        "tiendas": tiendas.to_dict("records"),
        "dia_semana": dow[["dia", "txns", "indice"]].to_dict("records"),
        "dia_pico": {"fecha": str(dia_pico["date"]), "txns": int(dia_pico["txns"])},
        "dia_valle": {"fecha": str(dia_valle["date"]), "txns": int(dia_valle["txns"])},
        "top_productos": productos.to_dict("records"),
        "top_clientes": clientes.to_dict("records"),
        "categorias": categorias.to_dict("records"),
        "productos_sin_categoria": int(sin_categoria),
        "frecuencia": {k: round(float(v), 2) for k, v in freq.items()},
        "segmentos_compra": segmentos_compra.to_dict("records"),
        "pareto_pct": pareto,
        "busqueda_k": busqueda_k.to_dict("records"),
        "perfiles_cluster": perfiles.to_dict("records"),
        "pca_varianza_explicada": round(float(pca_var), 3),
        "reglas": reglas.to_dict("records"),
        "reglas_stats": {k: int(v) for k, v in reglas_stats.items()},
        "correlacion": correlacion.to_dict(),
    }


def as_markdown(d: dict) -> str:
    k = d["kpis"]
    cat = d["catalogo"]
    out = [
        "## Cifras del dataset (generadas, no escritas a mano)",
        "",
        "| Indicador | Valor |",
        "|---|---:|",
        f"| Unidades vendidas | {int(k['total_units']):,} |",
        f"| Transacciones | {int(k['total_transactions']):,} |",
        f"| Clientes únicos | {int(k['total_customers']):,} |",
        f"| Productos transaccionados | {int(k['total_products']):,} |",
        f"| Categorías observadas | {int(k['total_categories'])} |",
        f"| Canasta media | {d['canasta_media']} ítems |",
        f"| Período | {str(k['date_min'])[:10]} → {str(k['date_max'])[:10]} |",
    ]
    if cat:
        out += [
            f"| Productos en el catálogo | {cat['productos_en_catalogo']:,} |",
            f"| Mapeos producto→categoría | {cat['mapeos_producto_categoria']:,} |",
            f"| Categorías en el catálogo | {cat['categorias_en_catalogo']} |",
        ]
    out += [
        "",
        "### Transacciones por día de la semana",
        "",
        "| Día | Transacciones | Índice (Mié = 100) |",
        "|---|---:|---:|",
    ]
    out += [f"| {r['dia']} | {int(r['txns']):,} | {int(r['indice'])} |" for r in d["dia_semana"]]

    out += [
        "",
        "### Perfil de los clusters",
        "",
        "| Cluster | Clientes | % | Frecuencia | Unidades | Productos | "
        "Categorías | Canasta | Recencia |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    out += [
        f"| {int(r['cluster_id'])} | {int(r['n_customers']):,} | {int(r['pct'])} % | "
        f"{r['frecuencia']} | {int(r['unidades'])} | {int(r['productos'])} | "
        f"{r['categorias']} | {r['canasta']} | {int(r['recencia'])} |"
        for r in d["perfiles_cluster"]
    ]

    out += [
        "",
        "### Selección de k",
        "",
        "| k | Silhouette (media) | Desv. típica | Submuestras |",
        "|---:|---:|---:|---:|",
    ]
    out += [
        f"| {int(r['k'])} | {r['media']} | {r['desviacion']} | {int(r['n_samples'])} |"
        for r in d["busqueda_k"]
    ]

    rs = d["reglas_stats"]
    out += [
        "",
        f"### Reglas de asociación ({rs['total']} totales, {rs['simples']} con antecedente simple)",
        "",
        "| Antecedente | Consecuente | Soporte | Confianza | Lift |",
        "|---|---:|---:|---:|---:|",
    ]
    out += [
        f"| {r['antecedent_label']} | {int(r['consequent_product_id'])} | "
        f"{r['soporte']} | {r['confianza']} | {r['lift']} |"
        for r in d["reglas"]
    ]
    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Emite JSON en lugar de Markdown")
    parser.add_argument("--out", type=Path, help="Fichero de salida")
    args = parser.parse_args()

    data = collect()
    text = (
        json.dumps(data, indent=2, ensure_ascii=False, default=str)
        if args.json
        else as_markdown(data)
    )
    if args.out:
        args.out.write_text(text, encoding="utf-8")
        print(f"escrito en {args.out}")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
