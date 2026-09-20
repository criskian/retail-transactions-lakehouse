"""Regresión del aplanado de reglas de asociación.

El bug: `models.py` hacía `explode()` sobre el antecedente de FP-Growth, que es
multi-ítem. La regla ``{3,16} -> {21}`` con confianza 0,575 se convertía en dos
filas —``3 -> 21`` y ``16 -> 21``, ambas con 0,575— que son **falsas** como
reglas individuales: esa confianza pertenece a la pareja.

Sobre el dataset real producía 327 filas para sólo 215 pares distintos, con el
par ``5 -> 10`` repetido 11 veces con 11 confianzas contradictorias.

Estos tests fijan las invariantes que impiden que vuelva a pasar.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

pytestmark = pytest.mark.spark


@pytest.fixture()
def rules(gold_dir: Path):
    con = duckdb.connect(":memory:")
    pattern = (gold_dir / "product_rules").as_posix() + "/**/*.parquet"
    con.execute(f"create view rules as select * from read_parquet('{pattern}')")
    yield con
    con.close()


def test_conserva_el_antecedente_como_conjunto(rules):
    cols = {r[0] for r in rules.execute("describe rules").fetchall()}
    assert {"antecedent_ids", "antecedent_label", "antecedent_size"} <= cols, (
        "el antecedente debe persistirse como conjunto, no explotado en filas"
    )


def test_cada_regla_aparece_una_sola_vez(rules):
    """Ninguna combinación (antecedente, consecuente) puede repetirse."""
    duplicadas = rules.execute("""
        select count(*) from (
            select antecedent_ids, consequent_product_id
            from rules group by 1, 2 having count(*) > 1
        )
    """).fetchone()[0]
    assert duplicadas == 0


def test_no_hay_confianzas_contradictorias_para_la_misma_regla(rules):
    """La firma exacta del bug: misma regla, confianzas distintas."""
    conflictos = rules.execute("""
        select count(*) from (
            select antecedent_ids, consequent_product_id
            from rules
            group by 1, 2
            having count(distinct round(confidence, 6)) > 1
        )
    """).fetchone()[0]
    assert conflictos == 0


def test_el_consecuente_nunca_esta_en_el_antecedente(rules):
    malas = rules.execute(
        "select count(*) from rules where list_contains(antecedent_ids, consequent_product_id)"
    ).fetchone()[0]
    assert malas == 0


def test_metricas_en_rango(rules):
    fuera = rules.execute("""
        select count(*) from rules
        where confidence < 0 or confidence > 1
           or lift <= 0
           or (support is not null and (support < 0 or support > 1))
    """).fetchone()[0]
    assert fuera == 0


def test_la_etiqueta_describe_el_antecedente_completo(rules):
    filas = rules.execute(
        "select antecedent_ids, antecedent_label, antecedent_size from rules limit 200"
    ).fetchall()
    assert filas, "el dataset de prueba debería producir alguna regla"
    for ids, label, size in filas:
        assert size == len(ids)
        assert label == " + ".join(str(i) for i in ids)
