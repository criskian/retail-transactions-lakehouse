"""Los contratos de datos deben pasar sobre un lakehouse sano y fallar sobre uno roto."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from src.pipeline.quality import validate

pytestmark = pytest.mark.spark


def test_lakehouse_sano_supera_todos_los_contratos(gold_dir: Path):
    fallos = [r for r in validate(gold_dir) if not r.passed]
    assert not fallos, "\n".join(f"{r.name}: {r.detail}" for r in fallos)


def test_detecta_un_mart_vacio(gold_dir: Path, tmp_path: Path):
    copia = tmp_path / "gold"
    shutil.copytree(gold_dir, copia)
    roto = copia / "fact_sales_daily"
    shutil.rmtree(roto)
    roto.mkdir()  # exactamente lo que dejaba una escritura fallida

    fallos = {r.name for r in validate(copia) if not r.passed}
    assert "existe::fact_sales_daily" in fallos


def test_detecta_marts_de_modelos_ausentes(gold_dir: Path, tmp_path: Path):
    copia = tmp_path / "gold"
    shutil.copytree(gold_dir, copia)
    shutil.rmtree(copia / "product_rules")

    assert any(r.name == "existe::product_rules" and not r.passed for r in validate(copia))
    # Con --allow-missing-models deja de ser obligatorio.
    assert not any(r.name == "existe::product_rules" for r in validate(copia, require_models=False))
