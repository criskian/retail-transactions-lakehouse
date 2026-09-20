# 0005 — ECharts nativo y una paleta validada

**Estado:** aceptada · **Fecha:** 2026-09

## Contexto

La primera versión del dashboard usaba Plotly Express con una escala de color
distinta en cada gráfica (Blues, Greens, Purples, YlOrRd, Teal, Viridis,
Oranges, RdBu_r). El resultado no era ilegible, pero tampoco parecía un
producto: no había sistema.

## Decisión

**Gráficas:** `st.echarts_chart`, nativo desde Streamlit 1.5x. Da acceso a
Apache ECharts completo —mapa de calor de calendario, sunburst, sankey, radar,
boxplot— sin iframe ni dependencias de terceros.

> Se evaluó `streamlit-echarts` (el componente de la comunidad) y **está roto**
> con Streamlit 1.64: falla al declararse con
> `Component must be declared in pyproject.toml with asset_dir`. La API nativa
> lo hace innecesario.

**Color:** paleta validada por script, no elegida a ojo. Pasa banda de
luminosidad, piso de croma, separación para daltonismo y piso de visión normal
sobre las dos superficies.

| Comprobación (pares adyacentes) | Claro | Oscuro |
|---|---|---|
| ΔE mínimo con daltonismo | 9,1 | 8,4 |
| ΔE mínimo en visión normal | 19,6 | 19,3 |

**Tipografía:** Inter para la interfaz y JetBrains Mono para las cifras,
declaradas en `.streamlit/config.toml` (requiere Streamlit >= 1.50).

## Reglas que se derivan

- **Categórico** = identidad, en orden fijo, nunca ciclado.
- **Secuencial** = magnitud, un solo tono.
- **Divergente** = polaridad, sólo en la matriz de correlación (de -1 a 1), con
  gris neutro en el cero.
- **Nunca doble eje Y.** Dos medidas de escala distinta van en dos gráficas
  apiladas. Un segundo eje permite colocar el cruce donde uno quiera y es el
  error más común en paneles analíticos; la versión anterior lo ofrecía como
  opción y se retiró.

## Límite conocido

Con *todos* los pares de color en pantalla a la vez —una dispersión con cinco
clusters— sólo los tres primeros tonos superan el piso de distinguibilidad. Por
eso la proyección PCA se dibuja como **small multiples**: un panel por cluster,
con el resto de puntos en gris. Además de ser correcto, se lee mejor.

## Componentes de terceros

`streamlit-antd-components` aporta el menú lateral con iconos y
`streamlit-shadcn-ui` algunos contenedores. Ambos están confinados a la
presentación: si alguno rompe, `app/streamlit_app.py` cae de vuelta a
`st.radio` sin tocar el resto.
