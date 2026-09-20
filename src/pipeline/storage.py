"""Escritura de tablas con semántica *todo o nada*.

Motivación (bug real, reproducido): el código original borraba el directorio de
destino y *después* pedía a Spark que escribiera. Si la escritura fallaba a
mitad —por ejemplo `TASK_WRITE_FAILED` porque los Python workers no arrancan—
el mart quedaba como **directorio vacío**. Peor aún: el dashboard comprobaba
`path.exists()`, que para un directorio vacío es `True`, así que DuckDB
intentaba crear la vista, no encontraba ficheros y **tumbaba la aplicación
entera**, no sólo la página afectada.

Aquí se escribe primero a `<nombre>.__tmp` y sólo al terminar con éxito se
sustituye el directorio definitivo. Un fallo deja intacta la versión anterior.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import DataFrame

TMP_SUFFIX = ".__tmp"
OLD_SUFFIX = ".__old"


def has_data(path: Path) -> bool:
    """True si `path` contiene al menos un fichero Parquet.

    Es la comprobación correcta: `Path.exists()` devuelve True para un
    directorio vacío dejado por una escritura fallida.
    """
    return path.is_dir() and any(path.rglob("*.parquet"))


def write_parquet_atomic(
    df: DataFrame,
    path: Path,
    *,
    partition_by: str | list[str] | None = None,
    coalesce: int | None = None,
) -> None:
    """Escribe `df` en `path` de forma atómica."""
    path = Path(path)
    tmp = path.with_name(path.name + TMP_SUFFIX)
    old = path.with_name(path.name + OLD_SUFFIX)

    for leftover in (tmp, old):
        shutil.rmtree(leftover, ignore_errors=True)

    writer = df.coalesce(coalesce).write if coalesce else df.write
    writer = writer.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(partition_by)

    try:
        writer.parquet(str(tmp))
    except BaseException:
        # Si la escritura revienta, el temporal queda a medias: se borra antes de
        # propagar para no dejar basura que confunda la siguiente corrida.
        shutil.rmtree(tmp, ignore_errors=True)
        raise

    if not has_data(tmp):
        shutil.rmtree(tmp, ignore_errors=True)
        raise RuntimeError(
            f"La escritura de {path.name} no produjo ficheros Parquet; se conserva "
            f"la versión anterior."
        )

    # Swap. `Path.replace` no sirve con directorios no vacíos en Windows.
    if path.exists():
        path.rename(old)
    try:
        tmp.rename(path)
    except OSError:
        if old.exists():  # rollback
            old.rename(path)
        raise
    finally:
        shutil.rmtree(old, ignore_errors=True)


def cleanup_temp_dirs(root: Path) -> list[Path]:
    """Borra restos `.__tmp` / `.__old` de corridas interrumpidas."""
    removed = []
    for suffix in (TMP_SUFFIX, OLD_SUFFIX):
        for leftover in Path(root).glob(f"*{suffix}"):
            shutil.rmtree(leftover, ignore_errors=True)
            removed.append(leftover)
    return removed
