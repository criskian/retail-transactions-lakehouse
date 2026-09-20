"""CLI: orquesta bronze → silver → gold → models → export."""

from __future__ import annotations

import argparse
import time

from . import bronze, export_serving, gold, models, silver

STEPS = {
    "bronze": bronze.run,
    "silver": silver.run,
    "gold": gold.run,
    "models": models.run,
    "export": export_serving.run,
}

# `all` no incluye `export` por defecto: el artefacto de serving se construye
# explícitamente (en CI, o con `invoke export`) para no pagarlo en cada iteración
# local. Usa --step full para encadenarlo todo.
DEFAULT_ORDER = ["bronze", "silver", "gold", "models"]
FULL_ORDER = [*DEFAULT_ORDER, "export"]


def doctor() -> int:
    """Diagnostica el entorno sin levantar Spark."""
    from .spark_session import describe_environment, prepare_environment

    print("=== diagnóstico del entorno ===")
    try:
        prepare_environment()
    except RuntimeError as exc:
        print(f"\n[FALLO] {exc}")
        return 1
    for k, v in describe_environment().items():
        print(f"  {k:16s} {v}")
    print("\n[ok] el entorno puede ejecutar el pipeline.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Supermarket pipeline orchestrator")
    parser.add_argument(
        "--step",
        choices=[*STEPS, "all", "full"],
        default="all",
        help="Etapa a ejecutar. 'all' = bronze..models, 'full' = all + export.",
    )
    parser.add_argument(
        "--doctor",
        action="store_true",
        help="Verifica Java/Hadoop/Python workers y termina.",
    )
    args = parser.parse_args()

    if args.doctor:
        return doctor()

    if args.step == "all":
        order = DEFAULT_ORDER
    elif args.step == "full":
        order = FULL_ORDER
    else:
        order = [args.step]

    total = time.perf_counter()
    for name in order:
        t0 = time.perf_counter()
        print(f"\n=== running step: {name} ===")
        STEPS[name]()
        print(f"=== {name} done in {time.perf_counter() - t0:.1f}s ===")
    if len(order) > 1:
        print(f"\n=== pipeline completo en {time.perf_counter() - total:.1f}s ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
