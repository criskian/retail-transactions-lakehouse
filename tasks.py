"""Runner de tareas multiplataforma (reemplaza a `make` en Windows).

    pip install invoke
    invoke --list
    invoke pipeline
    invoke app

El Makefile se mantiene para Linux/macOS y CI; este archivo es la vía
recomendada en Windows, donde `make` no suele estar instalado.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from invoke import task

ROOT = Path(__file__).resolve().parent
VENV = ROOT / ".venv"
PY = VENV / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
PYTHON = str(PY) if PY.exists() else sys.executable


def _run(c, cmd: str, **kw):
    return c.run(cmd, pty=os.name != "nt", **kw)


# ---------------------------------------------------------------------------
# Entorno
# ---------------------------------------------------------------------------
@task(help={"dev": "Instala también las dependencias de desarrollo"})
def install(c, dev=False):
    """Crea el venv e instala dependencias (pipeline + serving)."""
    if not PY.exists():
        _run(c, f'"{sys.executable}" -m venv "{VENV}"')
    req = "requirements-dev.txt" if dev else "requirements-pipeline.txt"
    _run(c, f'"{PYTHON}" -m pip install --upgrade pip')
    _run(c, f'"{PYTHON}" -m pip install -r {req}')


@task
def winutils(c):
    """Descarga winutils.exe + hadoop.dll (solo Windows)."""
    _run(c, f'"{PYTHON}" scripts/setup_winutils.py')


@task
def doctor(c):
    """Diagnostica el entorno: Java, Hadoop, Python workers."""
    _run(c, f'"{PYTHON}" -m src.pipeline.run --doctor')


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
@task(help={"step": "bronze | silver | gold | models | all"})
def pipeline(c, step="all"):
    """Ejecuta el pipeline medallion."""
    _run(c, f'"{PYTHON}" -m src.pipeline.run --step {step}')


@task
def bronze(c):
    """Solo la capa Bronze."""
    pipeline(c, "bronze")


@task
def silver(c):
    """Solo la capa Silver."""
    pipeline(c, "silver")


@task
def gold(c):
    """Solo la capa Gold."""
    pipeline(c, "gold")


@task
def models(c):
    """Reentrena K-Means + FP-Growth + ALS."""
    pipeline(c, "models")


@task
def export(c):
    """Construye data/serving/serving.duckdb a partir de Gold."""
    _run(c, f'"{PYTHON}" -m src.pipeline.export_serving')


@task(help={"check": "Solo reporta cambios", "force": "Ejecuta aunque no haya cambios"})
def ingest(c, check=False, force=False):
    """Ingesta incremental desde data/landing/."""
    flag = "--check" if check else ("--force" if force else "--run")
    _run(c, f'"{PYTHON}" -m src.pipeline.ingest {flag}')


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
@task(help={"port": "Puerto HTTP (por defecto 8501)"})
def app(c, port=8501):
    """Levanta el dashboard Streamlit."""
    _run(c, f'"{PYTHON}" -m streamlit run app/streamlit_app.py --server.port {port}')


# ---------------------------------------------------------------------------
# Calidad
# ---------------------------------------------------------------------------
@task
def lint(c):
    """ruff check + format --check."""
    _run(c, f'"{PYTHON}" -m ruff check .')
    _run(c, f'"{PYTHON}" -m ruff format --check .')


@task
def fmt(c):
    """Formatea con ruff."""
    _run(c, f'"{PYTHON}" -m ruff check --fix .')
    _run(c, f'"{PYTHON}" -m ruff format .')


@task(help={"k": "Filtro -k de pytest"})
def test(c, k=None):
    """Corre la suite de tests."""
    extra = f" -k {k}" if k else ""
    _run(c, f'"{PYTHON}" -m pytest -q{extra}')


@task
def clean(c):
    """Borra capas derivadas y caches."""
    import shutil

    for d in ["data/bronze", "data/silver", "data/gold", "data/models", "data/serving"]:
        shutil.rmtree(ROOT / d, ignore_errors=True)
    for cache in ROOT.rglob("__pycache__"):
        shutil.rmtree(cache, ignore_errors=True)
    for cache in [".pytest_cache", ".ruff_cache"]:
        shutil.rmtree(ROOT / cache, ignore_errors=True)
    print("limpio")
