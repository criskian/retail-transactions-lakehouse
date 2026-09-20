"""La escritura atómica no debe dejar marts vacíos.

Regresión de un fallo real: el código antiguo borraba el directorio destino
*antes* de escribir. Cuando la escritura fallaba a mitad quedaba un directorio
vacío que hacía caer la aplicación entera al arrancar.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from py4j.protocol import Py4JJavaError

from src.pipeline.storage import cleanup_temp_dirs, has_data, write_parquet_atomic


def test_has_data_rechaza_directorio_vacio(tmp_path: Path):
    empty = tmp_path / "mart"
    empty.mkdir()
    assert empty.exists()  # `exists()` decía que sí...
    assert not has_data(empty)  # ...y por eso no sirve como comprobación.


def test_has_data_acepta_parquet(tmp_path: Path):
    mart = tmp_path / "mart" / "part=1"
    mart.mkdir(parents=True)
    (mart / "x.parquet").write_bytes(b"0")
    assert has_data(tmp_path / "mart")


def test_cleanup_borra_restos(tmp_path: Path):
    for name in ("a.__tmp", "b.__old", "c"):
        (tmp_path / name).mkdir()
    removed = cleanup_temp_dirs(tmp_path)
    assert {p.name for p in removed} == {"a.__tmp", "b.__old"}
    assert (tmp_path / "c").exists()


@pytest.mark.spark
def test_escritura_fallida_conserva_la_version_anterior(lakehouse: Path):
    """Si la nueva escritura revienta, el mart previo sigue consultable."""
    from src.pipeline.spark_session import get_spark

    spark = get_spark("test-atomic")
    try:
        target = lakehouse / "gold" / "_tmp_mart"
        write_parquet_atomic(spark.range(5).toDF("n"), target)
        assert has_data(target)
        antes = spark.read.parquet(str(target)).count()

        # `1/0` no sirve: fuera de modo ANSI, Spark devuelve NULL en vez de
        # fallar. `raise_error` sí aborta la tarea al materializar.
        malo = spark.range(3).selectExpr(
            "cast(id as int) as n",
            # El cast es necesario: sin él la columna es de tipo VOID y Parquet
            # la rechaza al planificar, antes de escribir nada.
            "cast(raise_error('fallo simulado') as string) as boom",
        )
        with pytest.raises(Py4JJavaError):
            write_parquet_atomic(malo, target)

        assert has_data(target), "el mart quedó vacío tras un fallo"
        assert spark.read.parquet(str(target)).count() == antes
        assert not (target.parent / "_tmp_mart.__tmp").exists()
    finally:
        spark.stop()
