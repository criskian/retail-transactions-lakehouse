"""Filtros globales (tiendas + rango de fechas) y los predicados SQL que generan.

Los filtros se construyen con los valores reales del dataset, así que añadir una
tienda nueva al lakehouse la hace aparecer sola, sin tocar código.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd
import streamlit as st

from . import data


@dataclass(frozen=True)
class Filters:
    stores: tuple[int, ...]
    all_stores: tuple[int, ...]
    start: date
    end: date
    date_min: date
    date_max: date

    @property
    def is_full_range(self) -> bool:
        return (
            self.start == self.date_min
            and self.end == self.date_max
            and set(self.stores) == set(self.all_stores)
        )

    def where(self, date_col: str = "date", store_col: str = "store_id") -> str:
        """Predicado SQL combinado. Devuelve `1=0` si no hay tiendas seleccionadas."""
        if not self.stores:
            return "1=0"
        stores = ", ".join(str(s) for s in self.stores)
        return (
            f"{date_col} between DATE '{self.start}' and DATE '{self.end}' "
            f"and {store_col} in ({stores})"
        )

    def previous_period(self) -> tuple[date, date]:
        """Ventana inmediatamente anterior, del mismo tamaño — para los deltas."""
        span = self.end - self.start
        prev_end = self.start - pd.Timedelta(days=1).to_pytimedelta()
        return prev_end - span, prev_end

    def where_previous(self, date_col: str = "date", store_col: str = "store_id") -> str:
        if not self.stores:
            return "1=0"
        p0, p1 = self.previous_period()
        stores = ", ".join(str(s) for s in self.stores)
        return f"{date_col} between DATE '{p0}' and DATE '{p1}' and {store_col} in ({stores})"


@st.cache_data(show_spinner=False)
def _bounds() -> dict:
    kpis = data.q("select * from fact_kpis").iloc[0]
    stores = data.q("select distinct store_id from fact_sales_daily order by store_id")[
        "store_id"
    ].tolist()
    return {
        "date_min": pd.to_datetime(kpis["date_min"]).date(),
        "date_max": pd.to_datetime(kpis["date_max"]).date(),
        "stores": [int(s) for s in stores],
    }


def render(container) -> Filters:
    """Dibuja los controles y devuelve el estado de filtrado."""
    b = _bounds()

    selected = container.multiselect(
        "Tiendas",
        options=b["stores"],
        default=b["stores"],
        help="Cada archivo `XXX_Tran.csv` del landing aparece aquí automáticamente.",
    )
    chosen = container.date_input(
        "Rango de fechas",
        value=(b["date_min"], b["date_max"]),
        min_value=b["date_min"],
        max_value=b["date_max"],
    )
    if isinstance(chosen, tuple) and len(chosen) == 2:
        start, end = chosen
    else:
        start, end = b["date_min"], b["date_max"]

    return Filters(
        stores=tuple(int(s) for s in selected),
        all_stores=tuple(b["stores"]),
        start=start,
        end=end,
        date_min=b["date_min"],
        date_max=b["date_max"],
    )


def guard(f: Filters) -> bool:
    """Muestra un aviso y devuelve False si el filtro no selecciona nada."""
    if not f.stores:
        st.warning("Selecciona al menos una tienda para ver datos.", icon=":material/filter_alt:")
        return False
    return True
