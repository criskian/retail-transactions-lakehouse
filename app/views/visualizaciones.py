"""Visualizaciones Analíticas: tendencia, dispersión y relaciones entre variables."""

from __future__ import annotations

import streamlit as st

from .. import data, theme
from ..filters import Filters

CUSTOMER_FEATURES = {
    "frequency": "Frecuencia",
    "units_total": "Unidades",
    "distinct_products": "Productos",
    "distinct_categories": "Categorías",
    "avg_basket_size": "Canasta",
    "recency_days": "Recencia",
}


def _timeseries(f: Filters) -> None:
    st.subheader("Serie de tiempo")
    grain = (
        st.segmented_control(
            "Granularidad",
            ["Diaria", "Semanal"],
            default="Diaria",
            key="ts_grain",
            label_visibility="collapsed",
        )
        or "Diaria"
    )

    bucket = "date" if grain == "Diaria" else "date_trunc('week', date)"
    ts = data.q(f"""
        select {bucket} as periodo,
               sum(units) as unidades,
               sum(txn_count) as transacciones
        from fact_sales_daily where {f.where()}
        group by 1 order by 1
    """)
    if ts.empty:
        st.info("Sin datos en el rango seleccionado.")
        return

    t = theme.tokens()
    labels = ts["periodo"].astype(str).tolist()
    zoom = [
        {"type": "inside"},
        {
            "type": "slider",
            "height": 16,
            "bottom": 0,
            "borderColor": "transparent",
            "fillerColor": t.grid,
        },
    ]

    # Dos medidas de escala distinta -> dos gráficas apiladas, nunca un doble eje:
    # un segundo eje Y permite "colocar" la intersección donde uno quiera y es
    # el error más común en paneles analíticos.
    for i, (col, titulo) in enumerate(
        [("transacciones", "Transacciones"), ("unidades", "Unidades")]
    ):
        st.echarts_chart(
            theme.base_option(
                xAxis=theme.axis(
                    "category",
                    data=labels,
                    axisLabel={"show": i == 1, "color": t.ink_muted, "fontSize": 11},
                ),
                yAxis=theme.axis(
                    "value", name=titulo, nameTextStyle={"color": t.ink_muted, "fontSize": 11}
                ),
                tooltip=theme.crosshair_tooltip(),
                dataZoom=zoom if i == 1 else [{"type": "inside"}],
                grid={
                    "left": 8,
                    "right": 16,
                    "top": 28,
                    "bottom": 40 if i == 1 else 8,
                    "containLabel": True,
                },
                series=[
                    theme.line_series(
                        ts[col].tolist(),
                        color=theme.series_color(i),
                        area=True,
                        markLine={
                            "silent": True,
                            "symbol": "none",
                            "lineStyle": {"color": t.axis, "type": "dashed", "width": 1},
                            "label": {"color": t.ink_muted, "fontSize": 10, "formatter": "media"},
                            "data": [{"type": "average"}],
                        },
                    )
                ],
            ),
            height=230 if i == 0 else 270,
            theme=None,
            key=f"ts_{col}",
        )

    with st.expander("Interpretación"):
        st.write(
            "Permite separar **tendencia** (crecimiento o caída sostenida) de "
            "**estacionalidad** (picos recurrentes). La línea punteada marca la "
            "media del rango filtrado. El volumen se concentra en fin de semana, "
            "lo que impacta directamente decisiones de personal y abastecimiento."
        )


def _boxplot(f: Filters) -> None:
    st.subheader("Distribución y atípicos")
    dim = (
        st.segmented_control(
            "Distribución de",
            ["Unidades por cliente", "Transacciones por cliente", "Unidades por categoría"],
            default="Unidades por cliente",
            key="box_dim",
            label_visibility="collapsed",
        )
        or "Unidades por cliente"
    )

    if dim == "Unidades por categoría":
        sql = f"""
            select coalesce(m.category_name, 'Sin categoría') as grupo,
                   sum(c.units) as valor
            from fact_category_daily c
            left join (select distinct category_id, category_name
                       from fact_category_metrics where category_name is not null) m
                using (category_id)
            where {f.where()} group by 1
        """
        unidad = "unidades"
    else:
        metric = "units" if dim.startswith("Unidades") else "txns"
        sql = f"""
            select customer_id::varchar as grupo, sum({metric}) as valor
            from fact_customer_daily where {f.where()} group by 1
        """
        unidad = "unidades" if metric == "units" else "transacciones"

    stats = data.q(f"""
        with base as ({sql})
        select
            min(valor) as minimo,
            quantile_cont(valor, 0.25) as q1,
            median(valor) as mediana,
            quantile_cont(valor, 0.75) as q3,
            max(valor) as maximo,
            avg(valor) as media,
            count(*) as n
        from base
    """).iloc[0]

    if not stats["n"]:
        st.info("Sin datos en el rango seleccionado.")
        return

    q1, q3 = float(stats["q1"]), float(stats["q3"])
    iqr = q3 - q1
    low = max(float(stats["minimo"]), q1 - 1.5 * iqr)
    high = min(float(stats["maximo"]), q3 + 1.5 * iqr)

    outliers = data.q(f"""
        with base as ({sql})
        select grupo, valor from base
        where valor > {high} or valor < {low}
        order by valor desc limit 400
    """)

    t = theme.tokens()
    st.echarts_chart(
        theme.base_option(
            grid={"left": 8, "right": 16, "top": 24, "bottom": 24, "containLabel": True},
            xAxis=theme.axis("category", data=[dim], axisLabel={"show": False}),
            yAxis=theme.axis(
                "value",
                name=unidad.capitalize(),
                nameTextStyle={"color": t.ink_muted, "fontSize": 11},
            ),
            tooltip={**theme.base_option()["tooltip"]},
            series=[
                {
                    "type": "boxplot",
                    "data": [[low, q1, float(stats["mediana"]), q3, high]],
                    "itemStyle": {
                        "color": t.surface,
                        "borderColor": theme.series_color(0),
                        "borderWidth": 2,
                    },
                    "boxWidth": [20, 60],
                },
                {
                    "type": "scatter",
                    "name": "Atípicos",
                    "data": [[0, float(v)] for v in outliers["valor"]],
                    "symbolSize": 6,
                    "itemStyle": {
                        "color": theme.series_color(1),
                        "opacity": 0.45,
                        "borderColor": t.surface,
                        "borderWidth": 2,
                    },
                },
            ],
        ),
        height=420,
        theme=None,
        key="boxplot",
    )

    cols = st.columns(5)
    for col, (label, value) in zip(
        cols,
        [
            ("Mínimo", stats["minimo"]),
            ("Q1", stats["q1"]),
            ("Mediana", stats["mediana"]),
            ("Q3", stats["q3"]),
            ("Máximo", stats["maximo"]),
        ],
        strict=False,
    ):
        col.metric(label, f"{float(value):,.0f}".replace(",", "."), border=True)

    with st.expander("Interpretación"):
        st.write(
            f"La caja cubre el rango intercuartílico; los bigotes llegan hasta "
            f"1,5·IQR. Los **{len(outliers)} puntos** fuera de ese rango son "
            f"comportamientos atípicos: clientes muy por encima de la mediana "
            f"(**{float(stats['mediana']):,.0f}** {unidad}) o categorías que "
            f"dominan el volumen. La media ({float(stats['media']):,.1f}) muy por "
            f"encima de la mediana confirma una distribución con cola larga a la "
            f"derecha.".replace(",", ".")
        )


def _correlation() -> None:
    st.subheader("Correlación entre variables de cliente")
    feats = data.q("select " + ", ".join(CUSTOMER_FEATURES) + " from dim_customer_features")
    corr = feats.corr(method="pearson")
    labels = [CUSTOMER_FEATURES[c] for c in corr.columns]

    cells = [
        [j, i, round(float(corr.iloc[i, j]), 2)] for i in range(len(corr)) for j in range(len(corr))
    ]

    t = theme.tokens()
    neg, mid, pos = t.diverging
    st.echarts_chart(
        theme.base_option(
            grid={"left": 8, "right": 8, "top": 8, "bottom": 60, "containLabel": True},
            xAxis=theme.axis(
                "category",
                data=labels,
                splitArea={"show": False},
                axisLabel={"color": t.ink_muted, "fontSize": 11, "rotate": 30},
            ),
            yAxis=theme.axis(
                "category",
                data=labels,
                splitArea={"show": False},
                axisLabel={"color": t.ink_muted, "fontSize": 11},
            ),
            tooltip={**theme.base_option()["tooltip"], "formatter": "{c}"},
            visualMap={
                # Divergente: dos tonos opuestos y gris neutro en el cero.
                "min": -1,
                "max": 1,
                "calculable": True,
                "orient": "horizontal",
                "left": "center",
                "bottom": 4,
                "itemWidth": 12,
                "itemHeight": 140,
                "textStyle": {"color": t.ink_muted, "fontSize": 11},
                "inRange": {"color": [*neg, mid, *pos]},
            },
            series=[
                {
                    "type": "heatmap",
                    "data": cells,
                    "label": {"show": True, "color": t.ink, "fontSize": 11},
                    "itemStyle": {"borderColor": t.surface, "borderWidth": 2},
                    "emphasis": {"itemStyle": {"borderColor": t.ink, "borderWidth": 2}},
                }
            ],
        ),
        height=480,
        theme=None,
        key="corr_heat",
    )

    with st.expander("Interpretación"):
        st.write(
            "- **Frecuencia y diversidad de productos** correlacionan fuerte: quien "
            "viene más también compra cosas más variadas, no sólo repite.\n"
            "- **La canasta media es casi independiente de la frecuencia**, lo que "
            "indica dos modos de compra distintos (frecuente de canasta pequeña vs. "
            "esporádico de canasta grande) y justifica un k > 2 en la segmentación.\n"
            "- **La recencia correlaciona negativamente con todo**: los clientes "
            "recientes son los más activos, así que las variables tienen contenido "
            "predictivo."
        )
    with st.expander("Ver como tabla"):
        st.dataframe(corr.round(3), width="stretch")


def render(f: Filters) -> None:
    st.title("Visualizaciones Analíticas")
    st.markdown(
        '<p class="section-note">Exploración de la estructura y el comportamiento '
        "de los datos.</p>",
        unsafe_allow_html=True,
    )
    _timeseries(f)
    st.divider()
    _boxplot(f)
    st.divider()
    _correlation()
