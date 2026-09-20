"""Contratos de datos sobre la capa Gold.

Se ejecuta después del pipeline y **falla la corrida** si algún mart no cumple
su contrato. Antes, un mart vacío o corrupto sólo se descubría cuando el
dashboard reventaba delante de alguien.

Dos niveles de comprobación:

* **Esquema** (pandera) — columnas, tipos y rangos de cada mart.
* **Invariantes entre tablas** — cosas que ningún esquema por sí solo puede
  ver: que cada cliente segmentado exista en las features, que los KPIs
  cuadren con los agregados diarios, que no haya reglas con confianza fuera
  de [0, 1].

Usa DuckDB y no Spark a propósito: así la validación corre también en el job de
tests, que no instala JVM.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb
import pandera.pandas as pa

from .paths import GOLD


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""


def _non_negative(col: str) -> pa.Column:
    return pa.Column(int, pa.Check.ge(0), nullable=False, coerce=True)


SCHEMAS: dict[str, pa.DataFrameSchema] = {
    "fact_kpis": pa.DataFrameSchema(
        {
            "total_units": _non_negative("total_units"),
            "total_transactions": _non_negative("total_transactions"),
            "total_customers": _non_negative("total_customers"),
            "total_products": _non_negative("total_products"),
            "total_categories": _non_negative("total_categories"),
        },
        strict=False,
    ),
    "dim_customer_features": pa.DataFrameSchema(
        {
            "customer_id": pa.Column(int, nullable=False, unique=True, coerce=True),
            "frequency": pa.Column(int, pa.Check.gt(0), nullable=False, coerce=True),
            "units_total": pa.Column(int, pa.Check.gt(0), nullable=False, coerce=True),
            "distinct_products": pa.Column(int, pa.Check.gt(0), nullable=False, coerce=True),
            "avg_basket_size": pa.Column(float, pa.Check.gt(0), nullable=False),
            "recency_days": pa.Column(int, pa.Check.ge(0), nullable=False, coerce=True),
        },
        strict=False,
    ),
    "fact_sales_daily": pa.DataFrameSchema(
        {
            "store_id": pa.Column(int, nullable=False, coerce=True),
            "units": pa.Column(int, pa.Check.gt(0), nullable=False, coerce=True),
            "txn_count": pa.Column(int, pa.Check.gt(0), nullable=False, coerce=True),
        },
        strict=False,
    ),
    "product_rules": pa.DataFrameSchema(
        {
            "antecedent_size": pa.Column(int, pa.Check.ge(1), nullable=False, coerce=True),
            "consequent_product_id": pa.Column(int, nullable=False, coerce=True),
            "confidence": pa.Column(float, pa.Check.in_range(0, 1), nullable=False),
            "lift": pa.Column(float, pa.Check.gt(0), nullable=False),
            "support": pa.Column(float, pa.Check.in_range(0, 1), nullable=True),
        },
        strict=False,
    ),
    "cluster_profiles": pa.DataFrameSchema(
        {
            "cluster_id": pa.Column(int, nullable=False, unique=True, coerce=True),
            "n_customers": pa.Column(int, pa.Check.gt(0), nullable=False, coerce=True),
        },
        strict=False,
    ),
}

# Tablas que deben existir con al menos una fila.
REQUIRED = [
    "fact_kpis",
    "fact_sales_daily",
    "fact_product_daily",
    "fact_category_daily",
    "fact_customer_daily",
    "dim_customer_features",
    "dim_product_features",
    "fact_category_metrics",
    "dim_customer_top_products",
]
MODEL_TABLES = [
    "cluster_assignments",
    "cluster_profiles",
    "cluster_pca",
    "kmeans_search",
    "product_rules",
    "customer_recommendations",
]

# (nombre, sql que debe devolver 0)
INVARIANTS = [
    (
        "cada cliente segmentado existe en dim_customer_features",
        """select count(*) from cluster_assignments a
           left join dim_customer_features c using (customer_id)
           where c.customer_id is null""",
    ),
    (
        "los KPIs cuadran con fact_sales_daily",
        """select case when (select total_transactions from fact_kpis)
                       = (select sum(txn_count) from fact_sales_daily)
                  then 0 else 1 end""",
    ),
    (
        "ninguna recomendación repite rank por cliente",
        """select count(*) from (
             select customer_id, rank from customer_recommendations
             group by 1, 2 having count(*) > 1)""",
    ),
    (
        "ninguna regla tiene el consecuente dentro del antecedente",
        """select count(*) from product_rules
           where list_contains(antecedent_ids, consequent_product_id)""",
    ),
    (
        "el historial top-10 no excede 10 filas por cliente",
        """select count(*) from (
             select customer_id from dim_customer_top_products
             group by 1 having count(*) > 10)""",
    ),
    (
        "no hay fechas fuera del rango declarado en fact_kpis",
        """select count(*) from fact_sales_daily, fact_kpis
           where date < date_min or date > date_max""",
    ),
]


def _register(con: duckdb.DuckDBPyConnection, gold: Path) -> list[str]:
    found = []
    for table in REQUIRED + MODEL_TABLES:
        path = gold / table
        if path.is_dir() and any(path.rglob("*.parquet")):
            con.execute(
                f"create view {table} as "
                f"select * from read_parquet('{path.as_posix()}/**/*.parquet')"
            )
            found.append(table)
    return found


def validate(gold: Path = GOLD, *, require_models: bool = True) -> list[CheckResult]:
    results: list[CheckResult] = []
    con = duckdb.connect(":memory:")
    found = _register(con, gold)

    expected = REQUIRED + (MODEL_TABLES if require_models else [])
    for table in expected:
        if table not in found:
            results.append(CheckResult(f"existe::{table}", False, "mart ausente o vacío"))
            continue
        n = con.execute(f"select count(*) from {table}").fetchone()[0]
        results.append(CheckResult(f"existe::{table}", n > 0, f"{n:,} filas" if n else "0 filas"))

    for table, schema in SCHEMAS.items():
        if table not in found:
            continue
        try:
            schema.validate(con.execute(f"select * from {table}").df(), lazy=True)
            results.append(CheckResult(f"esquema::{table}", True))
        except pa.errors.SchemaErrors as exc:
            cases = exc.failure_cases[["check", "column", "failure_case"]].head(5)
            results.append(CheckResult(f"esquema::{table}", False, cases.to_string(index=False)))

    for name, sql in INVARIANTS:
        tables_needed = [t for t in REQUIRED + MODEL_TABLES if t in sql]
        if any(t not in found for t in tables_needed):
            continue
        try:
            bad = con.execute(sql).fetchone()[0]
            results.append(
                CheckResult(
                    f"invariante::{name}",
                    bad == 0,
                    "" if bad == 0 else f"{bad:,} filas violan la regla",
                )
            )
        except duckdb.Error as exc:
            results.append(CheckResult(f"invariante::{name}", False, str(exc)))

    con.close()
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Valida los contratos de la capa Gold")
    parser.add_argument("--report", type=Path, help="Escribe el resultado como JSON")
    parser.add_argument("--allow-missing-models", action="store_true")
    args = parser.parse_args()

    results = validate(require_models=not args.allow_missing_models)
    failed = [r for r in results if not r.passed]

    for r in results:
        mark = "ok  " if r.passed else "FALLA"
        print(f"[{mark}] {r.name}" + (f" — {r.detail}" if r.detail else ""))

    summary = {
        "total": len(results),
        "passed": len(results) - len(failed),
        "failed": len(failed),
        "checks": [asdict(r) for r in results],
    }
    if args.report:
        args.report.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\n{summary['passed']}/{summary['total']} comprobaciones superadas")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
