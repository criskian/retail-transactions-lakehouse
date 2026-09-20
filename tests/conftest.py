"""Fixtures compartidas.

La suite construye un **lakehouse en miniatura** con datos sintéticos que tienen
la misma forma que los reales (mismo separador, misma estructura de canasta,
mismo catálogo producto→categoría). Así los tests no dependen del dataset de
55 MB ni de Git LFS, y CI puede ejecutarlos en menos de un minuto.
"""

from __future__ import annotations

import os
import random
import tempfile
from datetime import date, timedelta
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Aislamiento del lakehouse — DEBE ocurrir antes de importar nada de src.
#
# `src/pipeline/paths.py` resuelve las rutas **al importarse**. pytest importa
# los módulos de test (y con ellos `src.pipeline`) antes de ejecutar cualquier
# fixture, así que fijar la variable dentro de una fixture llega tarde: el
# pipeline de prueba escribiría sobre `data/` del repositorio. Pasó de verdad y
# dejó la capa Gold real con los modelos del dataset sintético.
# ---------------------------------------------------------------------------
REPO_DATA = (Path(__file__).resolve().parents[1] / "data").resolve()
TEST_DATA_ROOT = Path(tempfile.mkdtemp(prefix="lakehouse-tests-"))
os.environ["LAKEHOUSE_DATA"] = str(TEST_DATA_ROOT)

N_STORES = 2
N_CUSTOMERS = 400
N_PRODUCTS = 40
N_CATEGORIES = 6
N_DAYS = 45
BASKETS_PER_DAY = 40


def _write_dataset(landing: Path, seed: int = 7) -> None:
    rng = random.Random(seed)
    tx_dir = landing / "Transactions"
    prod_dir = landing / "Products"
    tx_dir.mkdir(parents=True, exist_ok=True)
    prod_dir.mkdir(parents=True, exist_ok=True)

    (prod_dir / "Categories.csv").write_text(
        "\n".join(f"{i}|CATEGORIA {i}" for i in range(1, N_CATEGORIES + 1)),
        encoding="utf-8",
    )
    # Un producto puede mapear a varias categorías, igual que en el catálogo real.
    lines = ["v.Code_pr|v.code"]
    for pid in range(1, N_PRODUCTS + 1):
        for cat in rng.sample(range(1, N_CATEGORIES + 1), rng.choice([1, 1, 2])):
            lines.append(f"{pid}|{cat}")
    (prod_dir / "ProductCategory.csv").write_text("\n".join(lines), encoding="utf-8")

    start = date(2013, 1, 1)
    # Productos "ancla" que co-ocurren, para que FP-Growth encuentre reglas
    # reales incluyendo alguna con antecedente de dos ítems.
    anchors = [1, 2, 3]
    for s in range(N_STORES):
        store_id = 100 + s
        rows = []
        for day in range(N_DAYS):
            d = start + timedelta(days=day)
            for _ in range(BASKETS_PER_DAY):
                customer = rng.randint(1, N_CUSTOMERS)
                basket = set(rng.sample(range(1, N_PRODUCTS + 1), rng.randint(2, 8)))
                if rng.random() < 0.55:
                    basket.update(anchors)
                if rng.random() < 0.30:
                    basket.add(rng.choice(anchors))
                items = list(basket)
                rng.shuffle(items)
                # Repetimos algún producto: así `qty` deja de ser siempre 1.
                if rng.random() < 0.4:
                    items.append(rng.choice(items))
                rows.append(f"{d.isoformat()}|{store_id}|{customer}|{' '.join(map(str, items))}")
        (tx_dir / f"{store_id}_Tran.csv").write_text("\n".join(rows), encoding="utf-8")


def _assert_isolated() -> None:
    """Red de seguridad: la suite jamás debe tocar los datos del repositorio."""
    from src.pipeline import paths

    if paths.DATA.resolve() == REPO_DATA:
        raise RuntimeError(
            "Los tests están apuntando a data/ del repositorio. "
            "LAKEHOUSE_DATA debe fijarse antes de importar src.pipeline.paths."
        )


@pytest.fixture(scope="session")
def synthetic_landing() -> Path:
    _write_dataset(TEST_DATA_ROOT / "landing")
    return TEST_DATA_ROOT


@pytest.fixture(scope="session")
def lakehouse(synthetic_landing: Path) -> Path:
    """Ejecuta el pipeline completo sobre los datos sintéticos, una sola vez."""
    pytest.importorskip("pyspark")
    _assert_isolated()

    # Hiperparámetros reducidos: el dataset de prueba es pequeño.
    os.environ.update(
        KMEANS_K_RANGE="2,3",
        KMEANS_SEARCH_SAMPLES="2",
        KMEANS_SAMPLE_FRACTION="0.5",
        PCA_SAMPLE_SIZE="500",
        FP_MIN_SUPPORT="0.10",
        FP_MIN_CONFIDENCE="0.30",
        FP_TOP_N_PRODUCTS="40",
        ALS_MAX_ITER="3",
        SPARK_DRIVER_MEMORY=os.environ.get("SPARK_DRIVER_MEMORY", "2g"),
        SPARK_SHUFFLE_PARTITIONS="2",
    )

    # Los módulos leen las rutas al importarse, así que se importan aquí.
    from src.pipeline import bronze, gold, models, silver

    try:
        bronze.run()
        silver.run()
        gold.run()
        models.run()
    except RuntimeError as exc:  # entorno sin JDK compatible o sin winutils
        pytest.skip(f"entorno Spark no disponible: {exc}")

    return synthetic_landing


@pytest.fixture(scope="session")
def serving_db(lakehouse: Path) -> Path:
    from src.pipeline import export_serving

    return export_serving.build(lakehouse / "serving" / "serving.duckdb", gold=lakehouse / "gold")


@pytest.fixture()
def gold_dir(lakehouse: Path) -> Path:
    return lakehouse / "gold"
