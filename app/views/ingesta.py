"""Generación de nuevos resultados (RF-8).

Esta página tiene dos modos:

* **local** — hay Spark disponible, así que se puede subir un CSV y disparar el
  pipeline de verdad como un subproceso.
* **desplegado** — la app vive en un entorno sin JVM y con sistema de ficheros
  efímero. Aquí no se ejecuta nada: se explica el mecanismo y se muestra el
  histórico de corridas que viene embebido en el artefacto de datos.

El modo se detecta, no se configura a mano.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
LANDING = ROOT / "data" / "landing"
LANDING_TX = LANDING / "Transactions"
LANDING_PRODUCTS = LANDING / "Products"
MANIFEST = LANDING / "_manifest.json"
RUNS_LOG = LANDING / "_runs.jsonl"

GITHUB_REPO = os.environ.get("GITHUB_REPO", "criskian/retail-transactions-lakehouse")


def spark_available() -> bool:
    """¿Se puede ejecutar el pipeline desde aquí?"""
    if os.environ.get("PIPELINE_ENABLED") == "0":
        return False
    try:
        import pyspark  # noqa: F401
    except ImportError:
        return False
    return LANDING.exists()


def _read_manifest() -> dict:
    if not MANIFEST.exists():
        return {}
    try:
        return json.loads(MANIFEST.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _read_runs() -> pd.DataFrame:
    if not RUNS_LOG.exists():
        return pd.DataFrame()
    rows = []
    for line in RUNS_LOG.read_text(encoding="utf-8").splitlines():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Explicación del mecanismo (ambos modos)
# ---------------------------------------------------------------------------
def _how_it_works() -> None:
    st.subheader("Cómo funciona")
    cols = st.columns(4)
    steps = [
        (
            ":material/upload_file:",
            "Detección",
            "Se calcula el `sha256` de cada CSV del landing y se compara con el manifest.",
        ),
        (
            ":material/account_tree:",
            "Reproceso",
            "Si algo es nuevo, cambió o desapareció, se relanza bronze → silver → gold → models.",
        ),
        (
            ":material/inventory_2:",
            "Publicación",
            "Se exporta `serving.duckdb` (~26 MB) con los marts que consume el dashboard.",
        ),
        (
            ":material/verified:",
            "Consistencia",
            "El manifest sólo se actualiza si la corrida termina bien; si falla, se reintenta.",
        ),
    ]
    for col, (icon, title, body) in zip(cols, steps, strict=False):
        with col, st.container(border=True):
            st.markdown(f"{icon} **{title}**")
            st.caption(body)


def _manifest_table() -> None:
    manifest = _read_manifest()
    files = []
    for d in (LANDING_TX, LANDING_PRODUCTS):
        if not d.exists():
            continue
        for p in sorted(d.glob("*.csv")):
            rel = p.relative_to(LANDING).as_posix()
            files.append(
                {
                    "Archivo": rel,
                    "Tamaño (MB)": round(p.stat().st_size / (1024 * 1024), 2),
                    "En manifest": "Sí" if rel in manifest else "No",
                }
            )
    if files:
        st.dataframe(pd.DataFrame(files), width="stretch", hide_index=True)
    else:
        st.info("No hay archivos en `data/landing/`.", icon=":material/folder_off:")


def _runs_table() -> None:
    st.subheader("Histórico de corridas")
    runs = _read_runs()
    if runs.empty:
        st.info("Todavía no hay corridas registradas.", icon=":material/history:")
        return

    runs = runs.sort_values("started_at", ascending=False)
    for _, run in runs.head(8).iterrows():
        status = run.get("status", "ok")
        icon, color = {
            "ok": (":material/check_circle:", "normal"),
            "failed": (":material/error:", "inverse"),
        }.get(status, (":material/info:", "off"))
        timings = run.get("timings_s") or {}
        total = sum(timings.values()) if isinstance(timings, dict) else 0
        with st.container(border=True):
            head, metric = st.columns([3, 1])
            head.markdown(f"{icon} **{run['started_at'][:19].replace('T', ' ')} UTC**")
            changed = (run.get("new") or []) + (run.get("changed") or [])
            head.caption(
                f"{len(changed)} archivo(s) afectado(s)"
                + (f" · {', '.join(changed[:3])}" if changed else "")
            )
            metric.metric("Duración", f"{total:.0f} s", delta_color=color)
            if isinstance(timings, dict) and timings:
                st.caption(" · ".join(f"{k} {v:.0f}s" for k, v in timings.items()))
            if status == "failed":
                st.error(run.get("error", "Error desconocido"), icon=":material/error:")


# ---------------------------------------------------------------------------
# Modo local: ejecutar de verdad
# ---------------------------------------------------------------------------
def _run_cli(args: list[str]) -> int:
    log_lines: list[str] = []
    box = st.empty()
    proc = subprocess.Popen(
        [sys.executable, "-u", "-m", "src.pipeline.ingest", *args],
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    for line in proc.stdout:  # type: ignore[union-attr]
        log_lines.append(line.rstrip())
        del log_lines[:-400]
        box.code("\n".join(log_lines[-40:]), language="text")
    proc.wait()
    return proc.returncode


def _local_controls() -> None:
    st.subheader("Cargar datos nuevos")
    col_tx, col_prod = st.columns(2)
    with col_tx:
        up = st.file_uploader(
            "Transacciones (`XXX_Tran.csv`, separador `|`)", type=["csv"], key="up_tx"
        )
        if up is not None:
            LANDING_TX.mkdir(parents=True, exist_ok=True)
            (LANDING_TX / up.name).write_bytes(up.getbuffer())
            st.success(f"Guardado en `data/landing/Transactions/{up.name}`", icon=":material/save:")
    with col_prod:
        up2 = st.file_uploader(
            "Catálogo (`Categories.csv` / `ProductCategory.csv`)", type=["csv"], key="up_prod"
        )
        if up2 is not None:
            LANDING_PRODUCTS.mkdir(parents=True, exist_ok=True)
            (LANDING_PRODUCTS / up2.name).write_bytes(up2.getbuffer())
            st.success(f"Guardado en `data/landing/Products/{up2.name}`", icon=":material/save:")

    st.divider()
    st.subheader("Ejecutar")
    skip_models = st.toggle(
        "Omitir reentrenamiento de modelos",
        value=False,
        help="Más rápido: recalcula KPIs y visualizaciones, deja segmentación y "
        "recomendador con los modelos anteriores.",
    )
    c1, c2, c3 = st.columns(3)

    if c1.button("Comprobar cambios", width="stretch", icon=":material/search:"):
        _run_cli(["--check"])

    if c2.button(
        "Ejecutar si hay cambios", width="stretch", type="primary", icon=":material/play_arrow:"
    ):
        args = ["--run"] + (["--skip-models"] if skip_models else [])
        with st.status("Ejecutando pipeline…", expanded=True) as status:
            rc = _run_cli(args)
            if rc == 0:
                st.cache_data.clear()
                st.cache_resource.clear()
                status.update(label="Pipeline completado", state="complete")
            else:
                status.update(label=f"El pipeline falló (rc={rc})", state="error")

    if c3.button("Forzar reproceso", width="stretch", icon=":material/refresh:"):
        args = ["--force"] + (["--skip-models"] if skip_models else [])
        with st.status("Reprocesando…", expanded=True) as status:
            rc = _run_cli(args)
            if rc == 0:
                st.cache_data.clear()
                st.cache_resource.clear()
                status.update(label="Reproceso completado", state="complete")
            else:
                status.update(label=f"Falló (rc={rc})", state="error")


# ---------------------------------------------------------------------------
def render(_f=None) -> None:
    st.title("Generación de nuevos resultados")

    local = spark_available()
    if local:
        st.markdown(
            '<p class="section-note">Modo local: Spark disponible, el pipeline se '
            "ejecuta desde aquí.</p>",
            unsafe_allow_html=True,
        )
    else:
        st.info(
            "Esta demo corre sin Spark: lee un artefacto DuckDB de ~26 MB publicado "
            "por CI. El cómputo vive en GitHub Actions, que sí tiene JVM y regenera "
            "los datos cuando llegan archivos nuevos. Clona el repo para ejecutar el "
            "pipeline completo en local.",
            icon=":material/cloud_done:",
        )
        st.link_button(
            "Ver el workflow que regenera los datos",
            f"https://github.com/{GITHUB_REPO}/actions/workflows/pipeline.yml",
            icon=":material/open_in_new:",
        )

    _how_it_works()
    st.divider()

    if local:
        st.subheader("Estado del landing")
        _manifest_table()
        st.divider()
        _local_controls()
        st.divider()

    _runs_table()
