"""Rutas del lakehouse.

`DATA_ROOT` se puede sobreescribir con la variable de entorno `LAKEHOUSE_DATA`
para que los tests corran contra un directorio temporal sin tocar los datos
reales.
"""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA = Path(os.environ.get("LAKEHOUSE_DATA", ROOT / "data"))

LANDING = DATA / "landing"
LANDING_TX = LANDING / "Transactions"
LANDING_PRODUCTS = LANDING / "Products"
QUARANTINE = LANDING / "_quarantine"

BRONZE = DATA / "bronze"
SILVER = DATA / "silver"
GOLD = DATA / "gold"
MODELS = DATA / "models"
SERVING = DATA / "serving"

MANIFEST = LANDING / "_manifest.json"
RUNS_LOG = LANDING / "_runs.jsonl"


def ensure_dirs() -> None:
    """Crea las carpetas del lakehouse. Se invoca al importar."""
    for p in (BRONZE, SILVER, GOLD, MODELS, SERVING):
        p.mkdir(parents=True, exist_ok=True)


ensure_dirs()
