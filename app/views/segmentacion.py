"""Segmentación de clientes con K-Means."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from .. import data, theme

FEATURES = {
    "frequency": "Frecuencia",
    "units_total": "Unidades",
    "distinct_products": "Productos",
    "distinct_categories": "Categorías",
    "avg_basket_size": "Canasta",
    "recency_days": "Recencia",
}


def _label_for(row: pd.Series) -> tuple[str, str]:
    """Etiqueta de negocio derivada del perfil, no de un id fijo.

    Se calcula a partir de las medias del cluster para que siga teniendo sentido
    cuando el pipeline reentrene y cambie el número de segmentos.
    """
    freq = float(row["avg_frequency"])
    basket = float(row["avg_avg_basket_size"])
    recency = float(row["avg_recency_days"])

    if freq >= 25:
        return "VIP / power users", "Atención preferente y verificación de cuentas atípicas."
    if recency >= 60:
        return "Inactivos", "Segmento de mayor riesgo de fuga: campaña de reactivación."
    if basket >= 18:
        return "Canasta grande", "Compra esporádica de alto volumen: promociones por monto mínimo."
    if freq >= 8:
        return "Regulares activos", "Núcleo del negocio: programa de fidelización estándar."
    return "Ocasionales recientes", "Cupón de segunda compra con vigencia corta."


def _header(profiles: pd.DataFrame, search: pd.DataFrame) -> None:
    best = search.loc[search["silhouette_mean"].idxmax()]
    total = int(profiles["n_customers"].sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Clientes segmentados", f"{total:,}".replace(",", "."), border=True)
    c2.metric(
        "Segmentos (k)",
        int(best["k"]),
        border=True,
        help="Elegido por silhouette promedio sobre varias submuestras.",
    )
    # El ± va dentro del valor: como `delta` Streamlit le pinta una flecha de
    # tendencia, que aquí no significa nada.
    c3.metric(
        "Silhouette",
        f"{float(best['silhouette_mean']):.3f} ± {float(best['silhouette_std']):.3f}",
        border=True,
        help="Cohesión frente a separación, de −1 a 1. El ± es la desviación "
        "típica entre submuestras.",
    )
    c4.metric("Variables", len(FEATURES), border=True, help=", ".join(FEATURES.values()))


def _k_search(search: pd.DataFrame) -> None:
    st.subheader("Cómo se eligió k")
    search = search.sort_values("k")
    best_k = int(search.loc[search["silhouette_mean"].idxmax(), "k"])
    t = theme.tokens()

    colors = [theme.series_color(0) if int(k) == best_k else t.sequential[1] for k in search["k"]]
    st.echarts_chart(
        theme.base_option(
            xAxis=theme.axis("category", data=[f"k = {int(k)}" for k in search["k"]]),
            yAxis=theme.axis(
                "value",
                min=0,
                max=1,
                name="Silhouette",
                nameTextStyle={"color": t.ink_muted, "fontSize": 11},
            ),
            tooltip={**theme.base_option()["tooltip"], "formatter": "{b}<br/>{c}"},
            series=[
                {
                    "type": "bar",
                    "data": [
                        {
                            "value": round(float(v), 4),
                            "itemStyle": {"color": c, "borderRadius": [4, 4, 0, 0]},
                        }
                        for v, c in zip(search["silhouette_mean"], colors, strict=False)
                    ],
                    "barMaxWidth": 56,
                    "label": {
                        "show": True,
                        "position": "top",
                        "color": t.ink_secondary,
                        "fontSize": 11,
                        "formatter": "{c}",
                    },
                }
            ],
        ),
        height=300,
        theme=None,
        key="k_search",
    )
    n = int(search["n_samples"].iloc[0])
    st.markdown(
        f'<p class="section-note">Promedio de <b>{n} submuestras</b> del 10 % de los '
        f"clientes. Con una sola muestra el ganador oscilaba entre k=4 y k=5, así que "
        f"la media evita que la segmentación cambie de forma con cualquier dato nuevo. "
        f"El modelo final se reentrena sobre los clientes completos con k={best_k}.</p>",
        unsafe_allow_html=True,
    )
    with st.expander("Ver como tabla"):
        st.dataframe(
            search.rename(
                columns={
                    "k": "k",
                    "silhouette_mean": "Silhouette (media)",
                    "silhouette_std": "Desv. típica",
                    "n_samples": "Submuestras",
                }
            ).drop(columns=["is_best"], errors="ignore"),
            width="stretch",
            hide_index=True,
        )


def _pca_small_multiples(pca: pd.DataFrame, profiles: pd.DataFrame) -> None:
    st.subheader("Visualización del clustering (PCA 2D)")
    points = pca[pca["kind"] == "point"]
    centroids = pca[pca["kind"] == "centroid"]
    explained = float(pca["explained_variance"].iloc[0]) if not pca.empty else 0.0

    st.markdown(
        f'<p class="section-note">Las 6 variables proyectadas a 2 dimensiones '
        f"(<b>{explained:.0%}</b> de la varianza). Se dibuja un panel por segmento en "
        f"vez de un único gráfico de 5 colores: con todos los pares de color en "
        f"pantalla a la vez la paleta no supera el umbral de distinguibilidad, "
        f"y así además se ve la forma de cada grupo.</p>",
        unsafe_allow_html=True,
    )

    t = theme.tokens()
    backdrop = [
        [round(float(x), 3), round(float(y), 3)]
        for x, y in zip(points["pc1"], points["pc2"], strict=False)
    ]
    clusters = sorted(points["cluster_id"].unique())
    labels = {int(r["cluster_id"]): _label_for(r)[0] for _, r in profiles.iterrows()}

    cols = st.columns(min(3, len(clusters)))
    for i, cid in enumerate(clusters):
        sub = points[points["cluster_id"] == cid]
        cen = centroids[centroids["cluster_id"] == cid]
        color = theme.series_color(int(cid))
        with cols[i % len(cols)]:
            st.echarts_chart(
                theme.base_option(
                    title={
                        "text": f"Cluster {int(cid)} · {labels.get(int(cid), '')}",
                        "subtext": f"{len(sub):,} de la muestra".replace(",", "."),
                        "left": 4,
                        "top": 0,
                        "textStyle": {"color": t.ink, "fontSize": 13, "fontWeight": 600},
                        "subtextStyle": {"color": t.ink_muted, "fontSize": 11},
                    },
                    grid={"left": 8, "right": 12, "top": 52, "bottom": 8, "containLabel": True},
                    xAxis=theme.axis("value", axisLabel={"show": False}, splitLine={"show": False}),
                    yAxis=theme.axis("value", axisLabel={"show": False}),
                    tooltip={"show": False},
                    series=[
                        {
                            "type": "scatter",
                            "data": backdrop,
                            "symbolSize": 3,
                            "itemStyle": {"color": t.grid},
                            "silent": True,
                            "large": True,
                        },
                        {
                            "type": "scatter",
                            "symbolSize": 4,
                            "large": True,
                            "itemStyle": {"color": color},
                            "data": [
                                [round(float(x), 3), round(float(y), 3)]
                                for x, y in zip(sub["pc1"], sub["pc2"], strict=False)
                            ],
                        },
                        {
                            "type": "scatter",
                            "symbolSize": 16,
                            "symbol": "diamond",
                            "itemStyle": {
                                "color": color,
                                "borderColor": t.surface,
                                "borderWidth": 2,
                            },
                            "data": [
                                [round(float(x), 3), round(float(y), 3)]
                                for x, y in zip(cen["pc1"], cen["pc2"], strict=False)
                            ],
                        },
                    ],
                ),
                height=260,
                theme=None,
                key=f"pca_{cid}",
            )


def _radar(profiles: pd.DataFrame) -> None:
    st.subheader("Perfil de cada segmento")
    t = theme.tokens()
    # Un radar en un contenedor a ancho completo deja mucho aire a los lados.
    _, centro, _ = st.columns([1, 4, 1])
    avg_cols = [f"avg_{c}" for c in FEATURES]
    maxima = {c: float(profiles[c].max()) or 1.0 for c in avg_cols}

    indicators = [{"name": FEATURES[c], "max": 1.0} for c in FEATURES]
    series_data = []
    for _, row in profiles.iterrows():
        cid = int(row["cluster_id"])
        series_data.append(
            {
                "value": [round(float(row[f"avg_{c}"]) / maxima[f"avg_{c}"], 3) for c in FEATURES],
                "name": f"Cluster {cid} · {_label_for(row)[0]}",
                "lineStyle": {"width": 2, "color": theme.series_color(cid)},
                "itemStyle": {"color": theme.series_color(cid)},
                "areaStyle": {"opacity": 0.08, "color": theme.series_color(cid)},
            }
        )

    centro.echarts_chart(
        theme.base_option(
            legend={
                "bottom": 0,
                "icon": "roundRect",
                "itemWidth": 10,
                "itemHeight": 10,
                "textStyle": {"color": t.ink_secondary, "fontSize": 11},
            },
            tooltip={**theme.base_option()["tooltip"], "trigger": "item"},
            radar={
                "indicator": indicators,
                # Radio contenido: con 64 % las etiquetas de los ejes superiores
                # se salían del lienzo.
                "radius": "56%",
                "center": ["50%", "50%"],
                "axisName": {"color": t.ink_secondary, "fontSize": 11},
                "splitLine": {"lineStyle": {"color": t.grid}},
                "splitArea": {"show": False},
                "axisLine": {"lineStyle": {"color": t.grid}},
            },
            series=[{"type": "radar", "data": series_data, "symbolSize": 4}],
        ),
        height=460,
        theme=None,
        key="radar_profiles",
    )
    st.markdown(
        '<p class="section-note">Cada eje está normalizado al máximo entre segmentos, '
        "para que las 6 variables sean comparables pese a tener rangos muy distintos.</p>",
        unsafe_allow_html=True,
    )


def _cluster_cards(profiles: pd.DataFrame) -> None:
    st.subheader("Lectura de negocio")
    total = int(profiles["n_customers"].sum())
    cols = st.columns(min(3, len(profiles)))
    for i, (_, row) in enumerate(profiles.iterrows()):
        cid = int(row["cluster_id"])
        name, action = _label_for(row)
        n = int(row["n_customers"])
        with cols[i % len(cols)], st.container(border=True):
            st.markdown(
                f'<div style="display:flex;align-items:center;gap:.5rem;">'
                f'<span style="width:10px;height:10px;border-radius:3px;'
                f'background:{theme.series_color(cid)};display:inline-block;"></span>'
                f"<b>Cluster {cid}</b> · {name}</div>",
                unsafe_allow_html=True,
            )
            # Es una participación, no una variación: `delta` pintaría una
            # flecha de tendencia que aquí no significa nada.
            st.metric("Clientes", f"{n:,}".replace(",", "."))
            st.caption(
                f"**{n / total:.1%} del total** · "
                f"Frecuencia {float(row['avg_frequency']):.1f} · "
                f"Canasta {float(row['avg_avg_basket_size']):.1f} · "
                f"Recencia {float(row['avg_recency_days']):.0f} d"
            )
            st.write(action)

    with st.expander("Promedios y medianas por segmento"):
        st.dataframe(profiles.round(2), width="stretch", hide_index=True)


def _lookup() -> None:
    st.subheader("Consultar un cliente")
    cid = st.number_input("Customer ID", min_value=1, step=1, value=336296, key="seg_customer")
    res = data.q(f"""
        select a.cluster_id, c.*
        from cluster_assignments a
        join dim_customer_features c using (customer_id)
        where a.customer_id = {int(cid)}
    """)
    if res.empty:
        st.info(
            f"El cliente {int(cid)} no está en la tabla de segmentos.", icon=":material/search_off:"
        )
        return
    row = res.iloc[0]
    cluster = int(row["cluster_id"])
    st.success(f"Cliente {int(cid)} → **Cluster {cluster}**", icon=":material/person_search:")
    st.dataframe(res, width="stretch", hide_index=True)


def render(_f=None) -> None:
    st.title("Segmentación de Clientes")
    st.markdown(
        '<p class="section-note">K-Means sobre 6 variables de comportamiento. '
        "Los segmentos son globales: no dependen de los filtros de tienda y fecha.</p>",
        unsafe_allow_html=True,
    )

    profiles = data.q("select * from cluster_profiles order by cluster_id")
    search = data.q("select * from kmeans_search order by k")
    pca = data.q("select * from cluster_pca")

    _header(profiles, search)
    st.divider()
    _k_search(search)
    st.divider()
    _pca_small_multiples(pca, profiles)
    st.divider()
    _radar(profiles)
    st.divider()
    _cluster_cards(profiles)
    st.divider()
    _lookup()
