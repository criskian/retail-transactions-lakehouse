"""Sistema visual único del dashboard.

Toda la tinta del proyecto sale de aquí: los tokens de color, el orden
categórico, las rampas y las opciones base de ECharts. El dashboard anterior
usaba una escala distinta por gráfica (Blues, Greens, Purples, YlOrRd, Teal,
Viridis, Oranges, RdBu_r), que es justo lo que hace que un panel parezca
improvisado.

Reglas que este módulo hace cumplir
-----------------------------------
* **Categórico** = identidad. Orden fijo, nunca ciclado.
* **Secuencial** = magnitud. Un solo tono, claro → oscuro.
* **Divergente** = polaridad. Dos tonos + gris neutro en el centro (sólo para
  la matriz de correlación, que va de −1 a 1).
* El color de estado (bueno/alerta/crítico) nunca se reutiliza como serie.
* El texto lleva tokens de texto, nunca el color de la serie.

La paleta está **validada**, no elegida a ojo: pasa banda de luminosidad, piso
de croma, separación para daltonismo y piso de visión normal sobre ambas
superficies (``scripts/validate_palette.js`` del método de visualización).
Resultado en pares adyacentes — claro: ΔE CVD 9,1 / normal 19,6; oscuro: 8,4 /
19,3.

Límite conocido: con *todos* los pares en juego (dispersión, burbujas) sólo los
tres primeros tonos superan el piso. Por eso el scatter de clusters se dibuja
como *small multiples* —un panel por cluster— en vez de un único gráfico con
cinco colores simultáneos.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import streamlit as st


@dataclass(frozen=True)
class Tokens:
    """Tokens de un modo (claro u oscuro)."""

    surface: str
    plane: str
    ink: str
    ink_secondary: str
    ink_muted: str
    grid: str
    axis: str
    border: str
    categorical: list[str]
    sequential: list[str]
    diverging: tuple[list[str], str, list[str]]
    status: dict[str, str] = field(default_factory=dict)


# Rampa secuencial azul (100 → 700).
_BLUE_RAMP_LIGHT = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#2a78d6", "#256abf", "#184f95"]
_BLUE_RAMP_DARK = ["#0d366b", "#104281", "#184f95", "#256abf", "#3987e5", "#6da7ec", "#9ec5f4"]

_STATUS = {
    "good": "#0ca30c",
    "warning": "#fab219",
    "serious": "#ec835a",
    "critical": "#d03b3b",
}

LIGHT = Tokens(
    surface="#fcfcfb",
    plane="#f9f9f7",
    ink="#0b0b0b",
    ink_secondary="#52514e",
    ink_muted="#898781",
    grid="#e1e0d9",
    axis="#c3c2b7",
    border="rgba(11,11,11,0.10)",
    categorical=[
        "#2a78d6",
        "#eb6834",
        "#1baf7a",
        "#eda100",
        "#e87ba4",
        "#008300",
        "#4a3aa7",
        "#e34948",
    ],
    sequential=_BLUE_RAMP_LIGHT,
    diverging=(["#184f95", "#2a78d6", "#86b6ef"], "#f0efec", ["#f0a3a3", "#e34948", "#a32222"]),
    status=_STATUS,
)

DARK = Tokens(
    surface="#1a1a19",
    plane="#0d0d0d",
    ink="#ffffff",
    ink_secondary="#c3c2b7",
    ink_muted="#898781",
    grid="#2c2c2a",
    axis="#383835",
    border="rgba(255,255,255,0.10)",
    categorical=[
        "#3987e5",
        "#d95926",
        "#199e70",
        "#c98500",
        "#d55181",
        "#008300",
        "#9085e9",
        "#e66767",
    ],
    sequential=_BLUE_RAMP_DARK,
    diverging=(["#9ec5f4", "#3987e5", "#184f95"], "#383835", ["#a32222", "#e66767", "#f0a3a3"]),
    status=_STATUS,
)

# Los tres primeros tonos son los únicos que superan el piso con todos los pares
# en juego; cualquier forma que muestre todas las series a la vez (dispersión)
# debe respetarlo o recurrir a small multiples.
ALL_PAIRS_SAFE = 3


def tokens() -> Tokens:
    """Tokens del modo activo en Streamlit."""
    try:
        return DARK if st.context.theme.type == "dark" else LIGHT
    except Exception:
        return LIGHT


def series_color(index: int) -> str:
    """Color categórico por posición. Sigue a la entidad, nunca al ranking."""
    palette = tokens().categorical
    return palette[index % len(palette)]


def cluster_colors(n: int) -> list[str]:
    return [series_color(i) for i in range(n)]


# ---------------------------------------------------------------------------
# ECharts
# ---------------------------------------------------------------------------
def base_option(**overrides) -> dict:
    """Esqueleto común: rejilla recesiva, ejes finos, tooltip y animación."""
    t = tokens()
    option = {
        "backgroundColor": "transparent",
        "textStyle": {"fontFamily": "Inter, system-ui, sans-serif", "color": t.ink_secondary},
        "animationDuration": 500,
        "animationEasing": "cubicOut",
        "grid": {"left": 8, "right": 16, "top": 24, "bottom": 8, "containLabel": True},
        "tooltip": {
            "trigger": "item",
            "backgroundColor": t.surface,
            "borderColor": t.border,
            "borderWidth": 1,
            "textStyle": {"color": t.ink, "fontSize": 12},
            "extraCssText": "box-shadow:0 4px 16px rgba(0,0,0,.12);border-radius:10px;",
        },
    }
    option.update(overrides)
    return option


def axis(kind: str = "value", **overrides) -> dict:
    """Eje recesivo: sin línea propia, rejilla hairline, etiquetas apagadas."""
    t = tokens()
    a = {
        "type": kind,
        "axisLine": {"show": False},
        "axisTick": {"show": False},
        "axisLabel": {"color": t.ink_muted, "fontSize": 11},
        "splitLine": {"lineStyle": {"color": t.grid, "width": 1}},
    }
    if kind == "category":
        a["splitLine"] = {"show": False}
        a["axisLine"] = {"show": True, "lineStyle": {"color": t.axis}}
    a.update(overrides)
    return a


def crosshair_tooltip(**overrides) -> dict:
    """Tooltip con línea guía, para series temporales."""
    t = tokens()
    tip = base_option()["tooltip"] | {
        "trigger": "axis",
        "axisPointer": {"type": "line", "lineStyle": {"color": t.axis, "width": 1}},
    }
    tip.update(overrides)
    return tip


def bar_series(data, *, color: str | None = None, horizontal: bool = False, **overrides) -> dict:
    """Barra con extremo redondeado 4px anclado a la línea base."""
    radius = [0, 4, 4, 0] if horizontal else [4, 4, 0, 0]
    s = {
        "type": "bar",
        "data": data,
        "itemStyle": {"color": color or series_color(0), "borderRadius": radius},
        "barMaxWidth": 28,
        "emphasis": {"focus": "series"},
    }
    s.update(overrides)
    return s


def line_series(data, *, color: str | None = None, area: bool = False, **overrides) -> dict:
    """Línea de 2px, sin marcadores por punto."""
    c = color or series_color(0)
    s = {
        "type": "line",
        "data": data,
        "showSymbol": False,
        "symbolSize": 8,
        "lineStyle": {"width": 2, "color": c},
        "itemStyle": {"color": c},
        "smooth": False,
    }
    if area:
        s["areaStyle"] = {"opacity": 0.12, "color": c}
    s.update(overrides)
    return s


CSS = """
<style>
  .block-container { padding-top: 2.2rem; max-width: 1400px; }
  [data-testid="stMetricValue"] { font-variant-numeric: tabular-nums; letter-spacing: -0.02em; }
  [data-testid="stMetricLabel"] { text-transform: uppercase; letter-spacing: .06em; font-size: .72rem; }
  h1 { letter-spacing: -0.03em; font-weight: 700; }
  h2, h3 { letter-spacing: -0.015em; }
  .kpi-strip { display:flex; gap:.5rem; flex-wrap:wrap; margin:.25rem 0 1rem; }
  .section-note { color: var(--text-color-secondary, #6b7280); font-size:.85rem; margin:-.4rem 0 .9rem; }
  @media (max-width: 640px) { .block-container { padding-left: 16px; padding-right: 16px; } }
</style>
"""


def inject_css() -> None:
    st.markdown(CSS, unsafe_allow_html=True)
