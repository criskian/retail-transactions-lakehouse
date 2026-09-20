"""Recomendador: reglas de asociación (FP-Growth) y filtrado colaborativo (ALS)."""

from __future__ import annotations

import streamlit as st

from .. import data, theme


def _rules_for_product(pid: int, only_simple: bool) -> object:
    extra = "and antecedent_size = 1" if only_simple else ""
    return data.q(f"""
        select antecedent_label, antecedent_size,
               consequent_product_id,
               coalesce(consequent_category, 'Sin categoría') as categoria,
               support, confidence, lift
        from product_rules
        where list_contains(antecedent_ids, {int(pid)}) {extra}
        order by lift desc, confidence desc
        limit 20
    """)


def _tab_product() -> None:
    st.subheader("Dado un producto, qué se compra con él")
    st.markdown(
        '<p class="section-note">Reglas de FP-Growth con soporte ≥ 5 % y confianza '
        "≥ 30 % sobre el top-200 de productos. El antecedente puede ser un "
        "<b>conjunto</b> de productos: la regla <code>3 + 16 → 21</code> describe la "
        "pareja, no cada producto por separado.</p>",
        unsafe_allow_html=True,
    )

    products = data.q("""
        select distinct unnest(antecedent_ids) as product_id
        from product_rules order by 1
    """)["product_id"].tolist()

    if not products:
        st.info("No hay reglas disponibles con los umbrales configurados.")
        return

    col_sel, col_opt = st.columns([2, 1])
    pid = col_sel.selectbox("Producto", products, format_func=lambda p: f"Producto {p}")
    only_simple = col_opt.toggle(
        "Sólo reglas simples",
        value=False,
        help="Muestra únicamente reglas cuyo antecedente es un solo producto.",
    )

    recs = _rules_for_product(int(pid), only_simple)
    if recs.empty:
        st.info("Este producto no genera reglas con el umbral configurado.")
        return

    t = theme.tokens()
    top = recs.head(12).sort_values("lift")
    labels = [f"{r.antecedent_label} → {int(r.consequent_product_id)}" for r in top.itertuples()]
    st.echarts_chart(
        theme.base_option(
            grid={"left": 8, "right": 60, "top": 8, "bottom": 8, "containLabel": True},
            xAxis=theme.axis(
                "value",
                name="Lift",
                axisLabel={"show": False},
                splitLine={"show": False},
                nameTextStyle={"color": t.ink_muted, "fontSize": 11},
            ),
            yAxis=theme.axis(
                "category", data=labels, axisLabel={"color": t.ink_secondary, "fontSize": 11}
            ),
            tooltip={**theme.base_option()["tooltip"], "formatter": "{b}<br/>Lift: <b>{c}</b>"},
            series=[
                theme.bar_series(
                    [round(float(v), 2) for v in top["lift"]],
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
        height=max(280, 32 * len(top)),
        theme=None,
        key="rules_bar",
    )
    st.caption(
        "**Lift > 1** significa que el consecuente aparece junto al antecedente más "
        "de lo que cabría esperar por azar."
    )
    st.dataframe(
        recs.rename(
            columns={
                "antecedent_label": "Antecedente",
                "antecedent_size": "Ítems",
                "consequent_product_id": "Consecuente",
                "categoria": "Categoría",
                "support": "Soporte",
                "confidence": "Confianza",
                "lift": "Lift",
            }
        ).round(4),
        width="stretch",
        hide_index=True,
    )


def _tab_customer() -> None:
    st.subheader("Dado un cliente, qué recomendarle")
    st.markdown(
        '<p class="section-note">ALS implícito (rank=16, regParam=0.05). La cantidad '
        "comprada se interpreta como <i>confianza</i> del interés, no como una "
        "calificación.</p>",
        unsafe_allow_html=True,
    )

    cid = st.number_input("Customer ID", min_value=1, step=1, value=336296, key="rec_customer")

    history = data.q(f"""
        select h.product_id,
               coalesce(p.category_name, 'Sin categoría') as categoria,
               h.units, h.txns
        from dim_customer_top_products h
        left join (select distinct product_id, category_name
                   from dim_product_features) p using (product_id)
        where h.customer_id = {int(cid)}
        order by h.rank
    """)
    recs = data.q(f"""
        select r.product_id, r.score, r.rank,
               coalesce(p.category_name, 'Sin categoría') as categoria
        from customer_recommendations r
        left join (select distinct product_id, category_name
                   from dim_product_features) p using (product_id)
        where r.customer_id = {int(cid)}
        order by r.rank
    """)

    if history.empty and recs.empty:
        st.info(
            f"Sin historial ni recomendaciones para el cliente {int(cid)}.",
            icon=":material/search_off:",
        )
        return

    left, right = st.columns(2)
    with left:
        st.markdown("**Historial — top 10 por unidades**")
        if history.empty:
            st.caption("Sin historial registrado.")
        else:
            st.dataframe(
                history.rename(
                    columns={
                        "product_id": "Producto",
                        "categoria": "Categoría",
                        "units": "Unidades",
                        "txns": "Veces",
                    }
                ),
                width="stretch",
                hide_index=True,
            )
    with right:
        st.markdown("**Recomendado — top 10 ALS**")
        if recs.empty:
            st.caption("Cliente sin recomendaciones (cold start).")
        else:
            t = theme.tokens()
            top = recs.sort_values("score")
            st.echarts_chart(
                theme.base_option(
                    grid={"left": 8, "right": 56, "top": 8, "bottom": 8, "containLabel": True},
                    xAxis=theme.axis("value", axisLabel={"show": False}, splitLine={"show": False}),
                    yAxis=theme.axis(
                        "category", data=[f"Prod {int(p)}" for p in top["product_id"]]
                    ),
                    tooltip={
                        **theme.base_option()["tooltip"],
                        "formatter": "{b}<br/>Score: <b>{c}</b>",
                    },
                    series=[
                        theme.bar_series(
                            [round(float(s), 3) for s in top["score"]],
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
                height=340,
                theme=None,
                key="als_bar",
            )

    overlap = (
        set(history["product_id"]) & set(recs["product_id"])
        if not (history.empty or recs.empty)
        else set()
    )
    if not recs.empty:
        st.caption(
            f"{len(overlap)} de las 10 recomendaciones ya están en el historial del "
            f"cliente: el resto son productos nuevos para él."
        )


def _tab_global() -> None:
    st.subheader("Panorama de reglas")

    stats = data.q("""
        select count(*) as reglas,
               count(*) filter (where antecedent_size = 1) as simples,
               max(lift) as lift_max,
               avg(confidence) as conf_media
        from product_rules
    """).iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Reglas", f"{int(stats['reglas']):,}".replace(",", "."), border=True)
    c2.metric(
        "Antecedente simple",
        f"{int(stats['simples']):,}".replace(",", "."),
        border=True,
        help="El resto tiene antecedentes de 2 o más productos.",
    )
    c3.metric("Lift máximo", f"{float(stats['lift_max']):.2f}", border=True)
    c4.metric("Confianza media", f"{float(stats['conf_media']):.0%}", border=True)

    simple = data.q("""
        select antecedent_ids[1] as origen, consequent_product_id as destino,
               confidence, lift
        from product_rules
        where antecedent_size = 1
        order by lift desc limit 24
    """)

    if not simple.empty:
        t = theme.tokens()
        nodes, seen = [], set()
        for side, prefix in (("origen", "A"), ("destino", "B")):
            for p in simple[side]:
                name = f"{prefix}{int(p)}"
                if name not in seen:
                    seen.add(name)
                    nodes.append(
                        {
                            "name": name,
                            "label": {"formatter": f"Prod {int(p)}"},
                            "itemStyle": {
                                "color": theme.series_color(0 if prefix == "A" else 2),
                                "borderColor": t.surface,
                                "borderWidth": 2,
                            },
                        }
                    )
        links = [
            {
                "source": f"A{int(r.origen)}",
                "target": f"B{int(r.destino)}",
                "value": round(float(r.confidence), 3),
            }
            for r in simple.itertuples()
        ]
        st.echarts_chart(
            theme.base_option(
                tooltip={**theme.base_option()["tooltip"], "trigger": "item"},
                series=[
                    {
                        "type": "sankey",
                        "data": nodes,
                        "links": links,
                        "emphasis": {"focus": "adjacency"},
                        "nodeGap": 10,
                        "nodeWidth": 12,
                        "label": {"color": t.ink_secondary, "fontSize": 11},
                        "lineStyle": {"color": "gradient", "opacity": 0.3, "curveness": 0.5},
                    }
                ],
            ),
            height=520,
            theme=None,
            key="sankey_rules",
        )
        st.markdown(
            '<p class="section-note">Flujo de las 24 reglas de mayor lift con '
            "antecedente de un solo producto; el grosor es la confianza. Se excluyen "
            "las de antecedente múltiple porque un diagrama de flujo no puede "
            "representar honestamente una condición conjunta.</p>",
            unsafe_allow_html=True,
        )

    with st.expander("Todas las reglas, ordenadas por lift"):
        st.dataframe(
            data.q("""
                select antecedent_label as "Antecedente",
                       antecedent_size as "Ítems",
                       consequent_product_id as "Consecuente",
                       coalesce(consequent_category, 'Sin categoría') as "Categoría",
                       round(support, 4) as "Soporte",
                       round(confidence, 4) as "Confianza",
                       round(lift, 3) as "Lift"
                from product_rules order by lift desc, confidence desc
            """),
            width="stretch",
            hide_index=True,
            height=420,
        )


def render(_f=None) -> None:
    st.title("Recomendador de Productos")
    st.markdown(
        '<p class="section-note">Dos modelos complementarios: reglas de asociación '
        "para la canasta actual, filtrado colaborativo para el cliente. Ninguno "
        "depende de los filtros de tienda y fecha.</p>",
        unsafe_allow_html=True,
    )
    tab_a, tab_b, tab_c = st.tabs(
        [
            "Producto → productos",
            "Cliente → productos",
            "Panorama de reglas",
        ]
    )
    with tab_a:
        _tab_product()
    with tab_b:
        _tab_customer()
    with tab_c:
        _tab_global()
