"""Acceso a datos del dashboard.

La aplicación **no** depende de Spark ni del lakehouse completo: consulta un
único artefacto DuckDB de ~26 MB construido por `src/pipeline/export_serving.py`.
Eso es lo que permite desplegarla en un entorno sin JVM y con ~1 GB de RAM.

Resolución de la fuente, en orden:

1. ``SERVING_DUCKDB`` (ruta explícita) — útil en tests.
2. ``data/serving/serving.duckdb`` — desarrollo local tras `invoke export`.
3. Release de GitHub — el modo desplegado: se descarga una vez y se cachea.
4. Los Parquet de ``data/gold/`` — respaldo para quien acaba de correr el
   pipeline y todavía no exportó.

Las capas 1-3 abren el fichero en *read-only*: el sistema de ficheros del
entorno desplegado es efímero y la app nunca debe escribir en él.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

import duckdb
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
LOCAL_DUCKDB = ROOT / "data" / "serving" / "serving.duckdb"
GOLD = ROOT / "data" / "gold"

GITHUB_REPO = os.environ.get("GITHUB_REPO", "criskian/retail-transactions-lakehouse")
RELEASE_TAG = os.environ.get("SERVING_RELEASE_TAG", "serving-latest")
ASSET_NAME = "serving.duckdb"

# Tablas que la app espera encontrar. `optional` son las que produce la etapa de
# modelos: si falta alguna, la página correspondiente avisa en vez de tumbar
# toda la aplicación.
CORE_TABLES = [
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


class DataUnavailable(RuntimeError):
    """No hay ninguna fuente de datos utilizable."""


def _download_release_asset() -> Path | None:
    url = f"https://github.com/{GITHUB_REPO}/releases/download/{RELEASE_TAG}/{ASSET_NAME}"
    target = Path(tempfile.gettempdir()) / f"{RELEASE_TAG}-{ASSET_NAME}"
    if target.exists() and target.stat().st_size > 0:
        return target
    try:
        import requests

        with requests.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            tmp = target.with_suffix(".part")
            with tmp.open("wb") as f:
                shutil.copyfileobj(r.raw, f)
            tmp.rename(target)
        return target
    except Exception as exc:  # red caída, release inexistente, etc.
        st.session_state["_download_error"] = f"{type(exc).__name__}: {exc}"
        return None


def _has_parquet(path: Path) -> bool:
    """`Path.exists()` no sirve: un directorio vacío de una escritura fallida
    también existe, y era justo lo que tumbaba el dashboard entero."""
    return path.is_dir() and any(path.rglob("*.parquet"))


def _connect_from_parquet() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    found = []
    for table in CORE_TABLES + MODEL_TABLES:
        path = GOLD / table
        if not _has_parquet(path):
            continue
        con.execute(
            f"create view {table} as select * from read_parquet('{path.as_posix()}/**/*.parquet')"
        )
        found.append(table)

    missing_core = [t for t in CORE_TABLES if t not in found]
    if missing_core:
        raise DataUnavailable("Faltan marts Gold obligatorios: " + ", ".join(missing_core))
    return con


@st.cache_resource(show_spinner="Cargando datos…")
def get_con() -> duckdb.DuckDBPyConnection:
    explicit = os.environ.get("SERVING_DUCKDB")
    candidates = [Path(explicit)] if explicit else []
    candidates.append(LOCAL_DUCKDB)

    for path in candidates:
        if path.exists() and path.stat().st_size > 0:
            return duckdb.connect(str(path), read_only=True)

    if os.environ.get("SERVING_SOURCE", "auto") != "local":
        downloaded = _download_release_asset()
        if downloaded is not None:
            return duckdb.connect(str(downloaded), read_only=True)

    try:
        return _connect_from_parquet()
    except DataUnavailable as exc:
        raise DataUnavailable(
            f"{exc}\n\nNo se encontró `data/serving/serving.duckdb`, no se pudo "
            f"descargar el artefacto publicado y `data/gold/` está incompleto.\n"
            f"En local: `invoke pipeline && invoke export`."
        ) from exc


@st.cache_data(show_spinner=False)
def q(sql: str) -> object:
    """Ejecuta SQL y devuelve un DataFrame de pandas (cacheado por consulta)."""
    return get_con().execute(sql).df()


@st.cache_data(show_spinner=False)
def available_tables() -> set[str]:
    rows = get_con().execute("select table_name from information_schema.tables").fetchall()
    return {r[0] for r in rows}


def has_models() -> bool:
    return set(MODEL_TABLES).issubset(available_tables())


def missing_model_tables() -> list[str]:
    return sorted(set(MODEL_TABLES) - available_tables())


def source_label() -> str:
    """Descripción legible de de dónde salen los datos (para el pie de página)."""
    explicit = os.environ.get("SERVING_DUCKDB")
    if explicit and Path(explicit).exists():
        return f"DuckDB · {Path(explicit).name}"
    if LOCAL_DUCKDB.exists():
        return "DuckDB local · data/serving/serving.duckdb"
    if (Path(tempfile.gettempdir()) / f"{RELEASE_TAG}-{ASSET_NAME}").exists():
        return f"DuckDB · release `{RELEASE_TAG}`"
    return "Parquet · data/gold/"
