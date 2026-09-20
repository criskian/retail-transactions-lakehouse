"""Las cinco páginas del dashboard deben renderizar sin excepciones.

Es el test que más vale por línea: recorre todo el SQL de la aplicación contra
un artefacto real, así que detecta cualquier desajuste entre los marts que
produce el pipeline y las consultas que hace la interfaz.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("streamlit")
import streamlit as st
from streamlit.testing.v1 import AppTest

pytestmark = pytest.mark.spark

APP = Path(__file__).resolve().parents[1] / "app" / "streamlit_app.py"
PAGES = [
    "Resumen Ejecutivo",
    "Visualizaciones",
    "Segmentación",
    "Recomendador",
    "Nuevos datos",
]


def _clear_caches() -> None:
    """`get_con` es `@st.cache_resource`, y la caché es global al proceso.

    Sin limpiarla, un test hereda la conexión que abrió el anterior y deja de
    probar lo que cree estar probando.
    """
    st.cache_resource.clear()
    st.cache_data.clear()


@pytest.fixture()
def deployed_env(serving_db: Path, monkeypatch):
    """Simula el entorno desplegado: artefacto local, sin Spark."""
    monkeypatch.setenv("SERVING_DUCKDB", str(serving_db))
    monkeypatch.setenv("SERVING_SOURCE", "local")
    monkeypatch.setenv("PIPELINE_ENABLED", "0")
    _clear_caches()
    yield
    _clear_caches()


def test_arranca_sin_excepciones(deployed_env):
    at = AppTest.from_file(str(APP), default_timeout=180)
    at.run()
    assert not at.exception, [str(e.value) for e in at.exception]


@pytest.mark.parametrize("page", PAGES)
def test_cada_pagina_renderiza(deployed_env, page: str):
    at = AppTest.from_file(str(APP), default_timeout=180)
    at.run()
    assert not at.exception, [str(e.value) for e in at.exception]

    at.session_state["nav"] = page
    at.run()
    assert not at.exception, [f"{page}: {e.value}" for e in at.exception]


def test_los_filtros_se_propagan_al_sql(deployed_env):
    """Restringir el rango debe cambiar los KPIs, no decorarlos."""
    from app import data, filters

    f_todo = filters.Filters(
        stores=(100, 101),
        all_stores=(100, 101),
        start=__import__("datetime").date(2013, 1, 1),
        end=__import__("datetime").date(2013, 2, 14),
        date_min=__import__("datetime").date(2013, 1, 1),
        date_max=__import__("datetime").date(2013, 2, 14),
    )
    f_una = filters.Filters(
        stores=(100,),
        all_stores=(100, 101),
        start=f_todo.start,
        end=f_todo.end,
        date_min=f_todo.date_min,
        date_max=f_todo.date_max,
    )
    total = data.q(f"select sum(units) u from fact_sales_daily where {f_todo.where()}")
    parcial = data.q(f"select sum(units) u from fact_sales_daily where {f_una.where()}")
    assert float(parcial["u"].iloc[0]) < float(total["u"].iloc[0])


def test_sin_tiendas_no_devuelve_filas(deployed_env):
    from app import data, filters

    vacio = filters.Filters(
        stores=(),
        all_stores=(100,),
        start=__import__("datetime").date(2013, 1, 1),
        end=__import__("datetime").date(2013, 2, 1),
        date_min=__import__("datetime").date(2013, 1, 1),
        date_max=__import__("datetime").date(2013, 2, 1),
    )
    assert vacio.where() == "1=0"
    df = data.q(f"select count(*) n from fact_sales_daily where {vacio.where()}")
    assert int(df["n"].iloc[0]) == 0


def test_avisa_cuando_no_hay_datos(monkeypatch, tmp_path):
    """Sin fuente utilizable la app informa; no revienta con un stacktrace."""
    from app import data

    monkeypatch.setenv("SERVING_DUCKDB", str(tmp_path / "no-existe.duckdb"))
    monkeypatch.setenv("SERVING_SOURCE", "local")
    monkeypatch.setattr(data, "LOCAL_DUCKDB", tmp_path / "tampoco.duckdb")
    monkeypatch.setattr(data, "GOLD", tmp_path / "gold-vacio")
    _clear_caches()

    with pytest.raises(data.DataUnavailable, match="Faltan marts"):
        data.get_con()

    at = AppTest.from_file(str(APP), default_timeout=120)
    at.run()
    assert at.error, "debería mostrarse un mensaje de error legible"
    assert not at.exception, "el fallo debe presentarse como aviso, no como excepción"
    _clear_caches()
