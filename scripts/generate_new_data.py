"""Genera un CSV sintético en el mismo formato que los archivos de transacciones
del curso para demostrar la ingesta incremental (RF-8).

Uso:
    python scripts/generate_new_data.py

Crea:  data/landing/Transactions/115_Tran.csv
       (~5000 transacciones, tienda nueva 115, julio 2013)
"""

import csv
import random
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "data" / "landing" / "Transactions" / "115_Tran.csv"

random.seed(2026)

# Parámetros que imitan el dataset real
STORE_ID = 115
N_ROWS = 5_000
# Usamos los mismos rangos de IDs que el dataset original
CUSTOMER_IDS = list(range(1, 50_000))  # subconjunto de clientes reales
PRODUCT_IDS = list(range(1, 450))  # mismos productos (1–449)
START_DATE = date(2013, 7, 1)  # mes siguiente al dataset original
END_DATE = date(2013, 7, 31)


def random_date(start: date, end: date) -> str:
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).strftime("%Y-%m-%d")


def random_basket(min_items: int = 1, max_items: int = 20) -> str:
    n = random.randint(min_items, max_items)
    items = random.choices(PRODUCT_IDS, k=n)
    return " ".join(str(p) for p in items)


OUTPUT.parent.mkdir(parents=True, exist_ok=True)

with OUTPUT.open("w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f, delimiter="|")
    for _ in range(N_ROWS):
        writer.writerow(
            [
                random_date(START_DATE, END_DATE),
                STORE_ID,
                random.choice(CUSTOMER_IDS),
                random_basket(),
            ]
        )

print(f"Generado: {OUTPUT}")
print(f"  Filas    : {N_ROWS:,}")
print(f"  Tienda   : {STORE_ID}")
print(f"  Período  : {START_DATE} → {END_DATE}")
print()
print("Siguiente paso:")
print("  .venv\\Scripts\\python -m src.pipeline.ingest --run")
print("  O usa la página 'Generación de nuevos resultados' del dashboard.")
