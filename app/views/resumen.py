"""Resumen Ejecutivo: KPIs, rankings, estacionalidad y categorías."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from .. import data, theme
from ..filters import Filters


def _delta(current: float, previous: float) -> str | None:
    """Variación porcentual contra el período anterior del mismo tamaño."""
    if not previous:
        return None
    return f"{(current - previous) / previous:+.1%}"


def _kpis(f: Filters) -> None:
    now = data.q(f"""
        select coalesce(sum(units), 0) as units,
               coalesce(sum(txn_count), 0) as txns
        from fact_sales_daily where {f.where()}
    """).iloc[0]
    prev = data.q(f"""
        select coalesce(sum(units), 0) as units,
               coalesce(sum(txn_count), 0) as txns
        from fact_sales_daily where {f.where_previous()}
    """).iloc[0]

    customers = data.q(f"""
        select count(distinct customer_id) as n
        from fact_customer_daily where {f.where()}
    """).iloc[0]["n"]
    prev_customers = data.q(f"""
        select count(distinct customer_id) as n
        from fact_customer_daily where {f.where_previous()}
    """).iloc[0]["n"]

    basket = float(now["units"]) / float(now["txns"]) if now["txns"] else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(
        "Unidades vendidas",
        f"{int(now['units']):,}".replace(",", "."),
        _delta(now["units"], prev["units"]),
        border=True,
        help="Suma de cantidades en el período filtrado, contra el período anterior "
        "de igual duración.",
    )
    c2.metric(
        "Transacciones",
        f"{int(now['txns']):,}".replace(",", "."),
        _delta(now["txns"], prev["txns"]),
        border=True,
        help="Canastas únicas (cliente × tienda × fecha).",
    )
    c3.metric(
        "Clientes únicos",
        f"{int(customers):,}".replace(",", "."),
        _delta(customers, prev_customers),
        border=True,
    )
    c4.metric("Canasta media", f"{basket:.2f} ítems", border=True, help="Unidades por transacción.")


def _sparkline(f: Filters) -> None:
    ts = data.q(f"""
        select date, sum(txn_count) as txns
        from fact_sales_daily where {f.where()}
        group by 1 order by 1
    """)
    if ts.empty:
        return
    st.echarts_chart(
        theme.base_option(
            grid={"left": 0, "right": 0, "top": 4, "bottom": 0},
            xAxis=theme.axis(
                "category",
                data=ts["date"].astype(str).tolist(),
                axisLabel={"show": False},
                axisLine={"show": False},
            ),
            yAxis=theme.axis("value", axisLabel={"show": False}, splitLine={"show": False}),
            tooltip=theme.crosshair_tooltip(),
            series=[theme.line_series(ts["txns"].tolist(), area=True)],
        ),
        height=70,
        theme=None,
        key="spark_txns",
    )
    st.markdown(
        '<p class="section-note">Transacciones por día en el rango seleccionado.</p>',
        unsafe_allow_html=True,
    )


def _ranking(
    df: pd.DataFrame, label_col: str, value_col: str, *, key: str, value_name: str
) -> None:
    """Barras horizontales: magnitud → un solo tono, con etiqueta directa."""
    df = df.sort_values(value_col)
    t = theme.tokens()
    st.echarts_chart(
        theme.base_option(
            grid={"left": 8, "right": 72, "top": 8, "bottom": 8, "containLabel": True},
            xAxis=theme.axis("value", axisLabel={"show": False}, splitLine={"show": False}),
            yAxis=theme.axis("category", data=df[label_col].tolist()),
            tooltip={
                **theme.base_option()["tooltip"],
                "formatter": f"{{b}}<br/>{value_name}: <b>{{c}}</b>",
            },
            series=[
                theme.bar_series(
                    df[value_col].tolist(),
                    color=t.sequential[4],
                    horizontal=True,
                    label={
                        "show": True,
                        "position": "right",
                        "color": t.ink_secondary,
                        "fontSize": 11,
                        "formatter": "{c}",
                    },
                )
            ],
        ),
        height=max(260, 34 * len(df)),
        theme=None,
        key=key,
    )


def _rankings(f: Filters) -> None:
    left, right = st.columns(2)

    with left:
        st.subheader("Productos más vendidos")
        top_p = data.q(f"""
            select p.product_id,
                   coalesce(d.category_name, 'Sin categoría') as categoria,
                   sum(p.units) as unidades
            from fact_product_daily p
            left join (select distinct product_id, category_name
                       from dim_product_features) d using (product_id)
            where {f.where()}
            group by 1, 2 order by unidades desc limit 10
        """)
        if top_p.empty:
            st.info("Sin datos en el rango seleccionado.")
        else:
            top_p["label"] = "Prod " + top_p["product_id"].astype(str) + " · " + top_p["categoria"]
            _ranking(top_p, "label", "unidades", key="top_prod", value_name="Unidades")

    with right:
        st.subheader("Clientes más frecuentes")
        top_c = data.q(f"""
            select customer_id, sum(txns) as transacciones
            from fact_customer_daily where {f.where()}
            group by 1 order by transacciones desc limit 10
        """)
        if top_c.empty:
            st.info("Sin datos en el rango seleccionado.")
        else:
            top_c["label"] = "Cliente " + top_c["customer_id"].astype(str)
            _ranking(top_c, "label", "transacciones", key="top_cust", value_name="Transacciones")


def _peak_days(f: Filters) -> None:
    st.subheader("Días pico de compra")
    ts = data.q(f"""
        select date, sum(txn_count) as txns
        from fact_sales_daily where {f.where()}
        group by 1 order by 1
    """)
    if ts.empty:
        st.info("Sin datos en el rango seleccionado.")
        return

    t = theme.tokens()
    tab_cal, tab_line, tab_dow = st.tabs(["Calendario", "Serie de tiempo", "Día de la semana"])

    with tab_cal:
        cal_data = [
            [d.strftime("%Y-%m-%d"), int(v)] for d, v in zip(ts["date"], ts["txns"], strict=False)
        ]
        st.echarts_chart(
            theme.base_option(
                tooltip={**theme.base_option()["tooltip"], "formatter": "{c0}"},
                visualMap={
                    "min": int(ts["txns"].min()),
                    "max": int(ts["txns"].max()),
                    "orient": "horizontal",
                    "left": "center",
                    "bottom": 0,
                    "itemWidth": 12,
                    "itemHeight": 100,
                    "calculable": True,
                    "textStyle": {"color": t.ink_muted, "fontSize": 11},
                    "inRange": {"color": t.sequential},
                },
                calendar={
                    "top": 30,
                    "left": 40,
                    "right": 20,
                    "cellSize": ["auto", 16],
                    "range": [str(f.start), str(f.end)],
                    "splitLine": {"lineStyle": {"color": t.surface, "width": 2}},
                    "itemStyle": {"color": t.surface, "borderColor": t.surface, "borderWidth": 2},
                    "yearLabel": {"show": False},
                    "monthLabel": {"color": t.ink_muted, "fontSize": 11},
                    "dayLabel": {
                        "color": t.ink_muted,
                        "fontSize": 10,
                        "nameMap": ["D", "L", "M", "X", "J", "V", "S"],
                    },
                },
                series=[{"type": "heatmap", "coordinateSystem": "calendar", "data": cal_data}],
            ),
            height=230,
            theme=None,
            key="cal_heat",
        )
        st.markdown(
            '<p class="section-note">Cada celda es un día; el tono crece con el '
            "número de transacciones.</p>",
            unsafe_allow_html=True,
        )

    with tab_line:
        top5 = ts.nlargest(5, "txns")
        st.echarts_chart(
            theme.base_option(
                xAxis=theme.axis("category", data=ts["date"].astype(str).tolist()),
                yAxis=theme.axis(
                    "value",
                    name="Transacciones",
                    nameTextStyle={"color": t.ink_muted, "fontSize": 11},
                ),
                tooltip=theme.crosshair_tooltip(),
                dataZoom=[
                    {"type": "inside"},
                    {
                        "type": "slider",
                        "height": 18,
                        "bottom": 0,
                        "borderColor": "transparent",
                        "fillerColor": t.grid,
                    },
                ],
                grid={"left": 8, "right": 16, "top": 28, "bottom": 46, "containLabel": True},
                series=[
                    theme.line_series(
                        ts["txns"].tolist(),
                        markPoint={
                            "symbolSize": 42,
                            "itemStyle": {"color": theme.series_color(1)},
                            "label": {"color": "#fff", "fontSize": 10},
                            "data": [
                                {
                                    "name": str(d.date()),
                                    "coord": [ts.index.get_loc(i), int(v)],
                                    "value": int(v),
                                }
                                for i, d, v in zip(
                                    top5.index, top5["date"], top5["txns"], strict=True
                                )
                            ],
                        },
                    )
                ],
            ),
            height=340,
            theme=None,
            key="peak_line",
        )
        st.markdown(
            '<p class="section-note">Los cinco días de mayor volumen aparecen marcados.</p>',
            unsafe_allow_html=True,
        )

    with tab_dow:
        dow = data.q(f"""
            select dayofweek(date) as dow, sum(txn_count) as txns
            from fact_sales_daily where {f.where()}
            group by 1 order by 1
        """)
        names = ["Domingo", "Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado"]
        dow["nombre"] = dow["dow"].apply(lambda i: names[int(i)])
        # Reordenamos a la semana laboral latina (lunes primero).
        dow = pd.concat([dow[dow["dow"] != 0], dow[dow["dow"] == 0]])
        st.echarts_chart(
            theme.base_option(
                xAxis=theme.axis("category", data=dow["nombre"].tolist()),
                yAxis=theme.axis("value"),
                tooltip={
                    **theme.base_option()["tooltip"],
                    "formatter": "{b}<br/>Transacciones: <b>{c}</b>",
                },
                series=[
                    theme.bar_series(
                        dow["txns"].tolist(),
                        color=theme.tokens().sequential[4],
                        label={
                            "show": True,
                            "position": "top",
                            "color": t.ink_secondary,
                            "fontSize": 11,
                        },
                    )
                ],
            ),
            height=320,
            theme=None,
            key="dow_bar",
        )


def _categories(f: Filters) -> None:
    st.subheader("Categorías por volumen")
    st.markdown(
        '<p class="section-note">El dataset no trae precios, así que la '
        "rentabilidad se aproxima por volumen y frecuencia relativa.</p>",
        unsafe_allow_html=True,
    )
    cat = data.q(f"""
        select coalesce(m.category_name, 'Sin categoría') as categoria,
               sum(c.units) as unidades,
               sum(c.txns) as transacciones
        from fact_category_daily c
        left join (select distinct category_id, category_name
                   from fact_category_metrics where category_name is not null) m
            using (category_id)
        where {f.where()}
        group by 1 order by unidades desc
    """)
    if cat.empty:
        st.info("Sin datos en el rango seleccionado.")
        return

    t = theme.tokens()
    top = cat.head(12).sort_values("unidades")
    st.echarts_chart(
        theme.base_option(
            grid={"left": 8, "right": 90, "top": 8, "bottom": 8, "containLabel": True},
            xAxis=theme.axis("value", axisLabel={"show": False}, splitLine={"show": False}),
            yAxis=theme.axis(
                "category",
                data=top["categoria"].tolist(),
                axisLabel={"color": t.ink_secondary, "fontSize": 11},
            ),
            tooltip={**theme.base_option()["tooltip"], "formatter": "{b}<br/>Unidades: <b>{c}</b>"},
            series=[
                theme.bar_series(
                    top["unidades"].tolist(),
                    color=t.sequential[4],
                    horizontal=True,
                    label={
                        "show": True,
                        "position": "right",
                        "color": t.ink_secondary,
                        "fontSize": 11,
                    },
                )
            ],
        ),
        height=max(300, 30 * len(top)),
        theme=None,
        key="cat_bar",
    )

    with st.expander("Ver como tabla"):
        st.dataframe(cat, width="stretch", hide_index=True)


def render(f: Filters) -> None:
    st.title("Resumen Ejecutivo")
    _kpis(f)
    _sparkline(f)
    st.divider()
    _rankings(f)
    st.divider()
    _peak_days(f)
    st.divider()
    _categories(f)
