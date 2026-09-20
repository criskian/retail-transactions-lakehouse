"""La selección de k debe ser reproducible entre corridas.

`DataFrame.sample(seed=...)` no lo es: depende del orden físico de las filas
dentro de cada partición, así que regenerar Gold cambiaba la muestra y con ella
el silhouette. Se sustituyó por un muestreo por hash del identificador.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.spark


def test_la_submuestra_no_depende_del_particionado(lakehouse):
    from pyspark.sql import functions as F

    from src.pipeline.models import _deterministic_sample
    from src.pipeline.spark_session import get_spark

    spark = get_spark("test-sample")
    try:
        base = spark.range(0, 5000).select(F.col("id").cast("int").alias("customer_id"))

        una_particion = _deterministic_sample(base.repartition(1), 0.3, 42)
        muchas = _deterministic_sample(base.repartition(17), 0.3, 42)
        reordenada = _deterministic_sample(base.orderBy(F.col("customer_id").desc()), 0.3, 42)

        ids_1 = {r["customer_id"] for r in una_particion.collect()}
        ids_17 = {r["customer_id"] for r in muchas.collect()}
        ids_desc = {r["customer_id"] for r in reordenada.collect()}

        assert ids_1 == ids_17 == ids_desc
        # ~30 % con holgura estadística
        assert 0.25 < len(ids_1) / 5000 < 0.35

        otra_semilla = {r["customer_id"] for r in _deterministic_sample(base, 0.3, 99).collect()}
        assert otra_semilla != ids_1, "semillas distintas deben dar muestras distintas"
    finally:
        spark.stop()


def test_kmeans_search_devuelve_dispersion(gold_dir):
    """El mart de búsqueda debe reportar media y desviación, no un valor suelto."""
    import duckdb

    pattern = (gold_dir / "kmeans_search").as_posix() + "/**/*.parquet"
    con = duckdb.connect()
    try:
        cols = {
            r[0]
            for r in con.execute(f"describe select * from read_parquet('{pattern}')").fetchall()
        }
        assert {"k", "silhouette_mean", "silhouette_std", "n_samples"} <= cols
        n = con.execute(f"select min(n_samples) from read_parquet('{pattern}')").fetchone()[0]
        assert n >= 2, "el silhouette debe promediarse sobre varias submuestras"
    finally:
        con.close()
