"""Dashboard de analítica de transacciones de supermercado.

Arquitectura de la aplicación
-----------------------------
* `data.py` resuelve la fuente (DuckDB local, artefacto publicado o Parquet) y
  la abre en *read-only*. La app **no** depende de Spark.
* `theme.py` concentra la tinta: paleta validada, rampas y opciones de ECharts.
* `filters.py` mantiene los filtros globales y genera los predicados SQL.
* `views/` contiene una página por módulo del enunciado.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Streamlit ejecuta este fichero como script, así que pone `app/` en sys.path
# pero no la raíz del repositorio: sin esto `from app import ...` no resuelve.
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st

from app import data, filters, theme
from app.views import (
    ingesta,
    recomendador,
    resumen,
    segmentacion,
    visualizaciones,
)

st.set_page_config(
    page_title="Retail Lakehouse · Analítica",
    page_icon=":material/insights:",
    layout="wide",
    initial_sidebar_state="expanded",
)
theme.inject_css()

PAGES = {
    "Resumen Ejecutivo": ("bar-chart-line-fill", resumen.render, True),
    "Visualizaciones": ("graph-up", visualizaciones.render, True),
    "Segmentación": ("diagram-3-fill", segmentacion.render, False),
    "Recomendador": ("stars", recomendador.render, False),
    "Nuevos datos": ("cloud-arrow-up-fill", ingesta.render, False),
}


def _sidebar() -> tuple[str, filters.Filters | None]:
    with st.sidebar:
        st.markdown(
            '<div style="display:flex;align-items:center;gap:.6rem;margin-bottom:.2rem;">'
            '<span style="font-size:1.5rem;">◧</span>'
            '<div><div style="font-weight:700;font-size:1.05rem;letter-spacing:-.02em;">'
            "Retail Lakehouse</div>"
            '<div style="font-size:.75rem;opacity:.65;">Procesamiento distribuido</div>'
            "</div></div>",
            unsafe_allow_html=True,
        )
        st.divider()

        try:
            import streamlit_antd_components as sac

            page = sac.menu(
                [sac.MenuItem(name, icon=icon) for name, (icon, _, _) in PAGES.items()],
                key="nav",
                open_all=True,
                indent=14,
                size="sm",
            )
        except Exception:
            # Si el componente de terceros falla, la navegación nativa cubre el hueco.
            page = st.radio("Sección", list(PAGES), label_visibility="collapsed")

        page = page or next(iter(PAGES))

        f = None
        if PAGES[page][2]:
            st.divider()
            st.caption("FILTROS")
            f = filters.render(st.sidebar)

        st.divider()
        st.caption(f"Fuente: {data.source_label()}")
        return page, f


def main() -> None:
    try:
        data.get_con()
    except data.DataUnavailable as exc:
        st.title("Faltan los datos")
        st.error(str(exc), icon=":material/database_off:")
        st.stop()

    page, f = _sidebar()
    _icon, render, needs_filters = PAGES[page]

    if needs_filters:
        if not filters.guard(f):
            st.stop()
        render(f)
        return

    if page in ("Segmentación", "Recomendador") and not data.has_models():
        st.title(page)
        st.warning(
            "Los modelos todavía no se han entrenado. Faltan: "
            + ", ".join(f"`{t}`" for t in data.missing_model_tables())
            + ".\n\nEjecuta `invoke models` (o `invoke pipeline`) y vuelve a cargar.",
            icon=":material/model_training:",
        )
        st.stop()

    render(f)


main()
