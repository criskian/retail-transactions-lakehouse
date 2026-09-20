"""Exporta los marts Gold a un único artefacto DuckDB para la capa de serving.

Por qué existe
--------------
El dashboard desplegado no puede depender de Spark (JVM de ~500 MB + 317 MB de
wheel) ni del lakehouse completo: Silver pesa 379 MB. Esta etapa consolida sólo
lo que el dashboard consulta en **un fichero de ~26 MB** que se publica como
artefacto de CI y que la app abre en modo *read-only*.

Nota sobre el motor: aquí se usa DuckDB a propósito, no como atajo. DuckDB es
la capa de consulta declarada en la arquitectura; Spark sigue siendo el único
motor que **calcula** los marts. Esta etapa no transforma nada, sólo empaqueta.
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from .paths import GOLD, SERVING

# Tablas que la aplicación necesita. Se declaran explícitamente para que añadir
# un mart al pipeline no infle el artefacto de serving sin querer.
SERVING_TABLES = [
    # KPIs y series
    "fact_kpis",
    "fact_sales_daily",
    # Agregados que sustituyen las consultas contra Silver
    "fact_product_daily",
    "fact_category_daily",
    "fact_customer_daily",
    "dim_customer_top_products",
    # Dimensiones
    "dim_customer_features",
    "dim_product_features",
    "fact_category_metrics",
    # Modelos
    "cluster_assignments",
    "cluster_profiles",
    "cluster_pca",
    "kmeans_search",
    "product_rules",
    "customer_recommendations",
]

# Columnas por las que ordenar al materializar: mejora los *zone maps* de DuckDB
# y por tanto el pruning en las consultas filtradas por fecha.
CLUSTER_BY = {
    "fact_product_daily": "date, store_id",
    "fact_category_daily": "date, store_id",
    "fact_customer_daily": "date, store_id",
    "fact_sales_daily": "date, store_id",
    "dim_customer_top_products": "customer_id, rank",
    "customer_recommendations": "customer_id, rank",
    "cluster_assignments": "customer_id",
    "dim_customer_features": "customer_id",
}


def _has_data(path: Path) -> bool:
    return path.is_dir() and any(path.rglob("*.parquet"))


def build(target: Path | None = None, *, gold: Path = GOLD) -> Path:
    target = Path(target) if target else SERVING / "serving.duckdb"
    target.parent.mkdir(parents=True, exist_ok=True)

    missing = [t for t in SERVING_TABLES if not _has_data(gold / t)]
    if missing:
        raise FileNotFoundError(
            "Faltan marts Gold necesarios para el serving: "
            + ", ".join(missing)
            + "\nEjecuta el pipeline completo antes de exportar."
        )

    tmp = target.with_suffix(".duckdb.__tmp")
    tmp.unlink(missing_ok=True)

    con = duckdb.connect(str(tmp))
    try:
        for table in SERVING_TABLES:
            pattern = (gold / table).as_posix() + "/**/*.parquet"
            order = CLUSTER_BY.get(table)
            order_sql = f" order by {order}" if order else ""
            con.execute(
                f"create table {table} as select * from read_parquet('{pattern}'){order_sql}"
            )
            n = con.execute(f"select count(*) from {table}").fetchone()[0]
            print(f"[export] {table:28s} {n:>10,} filas")

        meta = {
            "built_at": datetime.now(UTC).isoformat(),
            "tables": SERVING_TABLES,
        }
        con.execute("create table _meta (key varchar, value varchar)")
        con.executemany(
            "insert into _meta values (?, ?)",
            [(k, json.dumps(v)) for k, v in meta.items()],
        )
        con.execute("checkpoint")
    finally:
        con.close()

    # En Windows no se puede reemplazar un fichero que otro proceso tiene
    # abierto, y el dashboard lo mantiene abierto mientras corre. El trabajo ya
    # está hecho en `tmp`, así que se conserva y se explica qué hacer.
    try:
        target.unlink(missing_ok=True)
        tmp.rename(target)
    except PermissionError as exc:
        raise PermissionError(
            f"No se pudo reemplazar {target}: hay otro proceso usándolo "
            f"(normalmente el dashboard).\n"
            f"Ciérralo y renombra {tmp.name} a {target.name}, o vuelve a "
            f"ejecutar la exportación."
        ) from exc

    size_mb = target.stat().st_size / 1024 / 1024
    print(f"[export] {target} — {size_mb:.1f} MB")
    return target


def run() -> None:
    build()


def main() -> int:
    parser = argparse.ArgumentParser(description="Exporta Gold → serving.duckdb")
    parser.add_argument("--out", type=Path, default=None, help="Ruta del fichero destino")
    args = parser.parse_args()
    build(args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
