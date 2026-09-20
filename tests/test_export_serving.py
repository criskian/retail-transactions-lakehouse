"""El artefacto de serving debe bastarse solo: ni Spark ni Silver."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.pipeline.export_serving import SERVING_TABLES

pytestmark = pytest.mark.spark


def test_contiene_todas_las_tablas_declaradas(serving_db: Path):
    con = duckdb.connect(str(serving_db), read_only=True)
    try:
        presentes = {
            r[0] for r in con.execute("select table_name from information_schema.tables").fetchall()
        }
        assert set(SERVING_TABLES) <= presentes
        for t in SERVING_TABLES:
            assert con.execute(f"select count(*) from {t}").fetchone()[0] > 0
    finally:
        con.close()


def test_no_incluye_silver(serving_db: Path):
    """Si Silver se colara, el artefacto pasaría de ~26 MB a cientos."""
    con = duckdb.connect(str(serving_db), read_only=True)
    try:
        tablas = {
            r[0] for r in con.execute("select table_name from information_schema.tables").fetchall()
        }
        assert "transactions_items" not in tablas
    finally:
        con.close()


def test_se_abre_en_solo_lectura(serving_db: Path):
    con = duckdb.connect(str(serving_db), read_only=True)
    try:
        with pytest.raises(duckdb.Error):
            con.execute("create table intruso (x int)")
    finally:
        con.close()


def test_falla_si_faltan_marts(tmp_path: Path):
    from src.pipeline.export_serving import build

    with pytest.raises(FileNotFoundError, match="Faltan marts"):
        build(tmp_path / "x.duckdb", gold=tmp_path / "gold-inexistente")
