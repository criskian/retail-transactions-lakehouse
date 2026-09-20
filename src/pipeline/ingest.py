"""Ingesta incremental: detecta datos nuevos y regenera los resultados (RF-8).

Mecanismo
---------
* Se mantiene un manifest ``data/landing/_manifest.json`` con ``ruta -> sha256``.
* En cada invocación se calcula el sha256 de los CSV bajo ``Transactions/`` y
  ``Products/``; si alguno es nuevo, cambió o desapareció, se relanza el
  pipeline completo y se actualiza el manifest.
* Cada corrida —incluidas las **fallidas**— se registra en
  ``data/landing/_runs.jsonl``.

Decisiones
----------
* **Reprocesar todo** en vez de calcular deltas por partición: el dataset cabe
  en memoria local y la consistencia entre marts y modelos queda garantizada por
  construcción. Documentado en ``docs/ADR/0004-reprocesado-completo.md``.
* El manifest guarda las rutas en formato POSIX. Antes usaba el separador del
  sistema, así que un manifest generado en Windows hacía que en macOS todos los
  ficheros pareciesen nuevos.
* Se toma un **lock** por fichero: dos pipelines simultáneos escribiendo sobre
  los mismos directorios dejarían el lakehouse corrupto.

CLI::

    python -m src.pipeline.ingest --check
    python -m src.pipeline.ingest --run
    python -m src.pipeline.ingest --force [--skip-models]
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import time
import traceback
from datetime import UTC, datetime
from pathlib import Path

from . import bronze, export_serving, gold, models, silver
from .paths import LANDING, LANDING_PRODUCTS, LANDING_TX, MANIFEST, RUNS_LOG

LOCK = LANDING / "_pipeline.lock"
LOCK_STALE_SECONDS = 4 * 60 * 60


def _now() -> str:
    return datetime.now(UTC).isoformat()


# ---------------------------------------------------------------------------
# Escaneo y manifest
# ---------------------------------------------------------------------------
def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _scan() -> dict[str, str]:
    out: dict[str, str] = {}
    for d in (LANDING_TX, LANDING_PRODUCTS):
        if not d.exists():
            continue
        for p in sorted(d.glob("*.csv")):
            if p.name.startswith("."):
                continue
            out[p.relative_to(LANDING).as_posix()] = _sha256(p)
    return out


def _load_manifest() -> dict[str, str]:
    if not MANIFEST.exists():
        return {}
    try:
        data = json.loads(MANIFEST.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    # Compatibilidad con manifests antiguos que guardaban separadores de Windows.
    return {k.replace("\\", "/"): v for k, v in data.items()}


def _save_manifest(m: dict[str, str]) -> None:
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(m, indent=2, sort_keys=True), encoding="utf-8")


def diff(
    current: dict[str, str], previous: dict[str, str]
) -> tuple[list[str], list[str], list[str]]:
    """Devuelve (nuevos, modificados, eliminados)."""
    new = [k for k in current if k not in previous]
    changed = [k for k in current if k in previous and current[k] != previous[k]]
    removed = [k for k in previous if k not in current]
    return sorted(new), sorted(changed), sorted(removed)


def _log_run(payload: dict) -> None:
    RUNS_LOG.parent.mkdir(parents=True, exist_ok=True)
    with RUNS_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload) + "\n")


# ---------------------------------------------------------------------------
# Lock
# ---------------------------------------------------------------------------
@contextlib.contextmanager
def _pipeline_lock():
    if LOCK.exists():
        age = time.time() - LOCK.stat().st_mtime
        if age < LOCK_STALE_SECONDS:
            holder = LOCK.read_text(encoding="utf-8").strip()
            raise RuntimeError(
                f"Ya hay un pipeline en ejecución ({holder}, hace {age / 60:.0f} min). "
                f"Si estás seguro de que no, borra {LOCK}."
            )
        print(f"[ingest] lock obsoleto ({age / 3600:.1f} h); se descarta.")

    LOCK.parent.mkdir(parents=True, exist_ok=True)
    LOCK.write_text(f"pid={os.getpid()} started={_now()}", encoding="utf-8")
    try:
        yield
    finally:
        LOCK.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Ejecución
# ---------------------------------------------------------------------------
def _run_pipeline(*, skip_models: bool = False, export: bool = True) -> dict[str, float]:
    timings: dict[str, float] = {}
    steps = [("bronze", bronze.run), ("silver", silver.run), ("gold", gold.run)]
    if not skip_models:
        steps.append(("models", models.run))
    if export:
        steps.append(("export", export_serving.run))

    for name, fn in steps:
        t0 = time.perf_counter()
        print(f"\n=== ingest: running {name} ===")
        fn()
        timings[name] = round(time.perf_counter() - t0, 2)
        print(f"=== {name} done in {timings[name]:.1f}s ===")
    return timings


def check() -> dict:
    current = _scan()
    new, changed, removed = diff(current, _load_manifest())
    report = {
        "files_seen": len(current),
        "new": new,
        "changed": changed,
        "removed": removed,
        "needs_run": bool(new or changed or removed),
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return report


def ingest(
    *, force: bool = False, skip_models: bool = False, export: bool = True, dry_run: bool = False
) -> dict:
    current = _scan()
    new, changed, removed = diff(current, _load_manifest())
    started_at = _now()

    if not (force or new or changed or removed):
        print("[ingest] sin archivos nuevos ni modificados; pipeline omitido.")
        return {
            "started_at": started_at,
            "ran": False,
            "status": "skipped",
            "new": [],
            "changed": [],
            "removed": [],
        }

    print(f"[ingest] cambios — nuevos={new} modificados={changed} eliminados={removed}")
    if dry_run:
        print("[ingest] --dry-run: no se ejecuta nada.")
        return {
            "started_at": started_at,
            "ran": False,
            "status": "dry-run",
            "new": new,
            "changed": changed,
            "removed": removed,
        }

    payload = {
        "started_at": started_at,
        "ran": True,
        "forced": force,
        "skip_models": skip_models,
        "new": new,
        "changed": changed,
        "removed": removed,
        "files_count": len(current),
    }

    with _pipeline_lock():
        try:
            payload["timings_s"] = _run_pipeline(skip_models=skip_models, export=export)
        except Exception as exc:
            # El manifest NO se actualiza: la próxima corrida reintentará. Pero sí
            # se deja constancia del fallo, que antes se perdía por completo.
            payload.update(
                status="failed",
                finished_at=_now(),
                error=f"{type(exc).__name__}: {exc}",
                traceback=traceback.format_exc(limit=20),
            )
            _log_run(payload)
            print(f"\n[ingest] FALLÓ: {payload['error']}")
            raise

    _save_manifest(current)
    payload.update(status="ok", finished_at=_now())
    _log_run(payload)
    print(f"[ingest] OK — {payload['files_count']} archivos procesados")
    return payload


def _build_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingesta incremental")
    g = parser.add_mutually_exclusive_group()
    g.add_argument("--check", action="store_true", help="Reporta cambios sin ejecutar")
    g.add_argument("--run", action="store_true", help="Ejecuta si hay cambios")
    g.add_argument("--force", action="store_true", help="Ejecuta aunque no haya cambios")
    parser.add_argument("--skip-models", action="store_true", help="Omite el reentrenamiento")
    parser.add_argument("--no-export", action="store_true", help="No reconstruye serving.duckdb")
    parser.add_argument("--dry-run", action="store_true", help="Muestra qué haría y termina")
    return parser


def main() -> int:
    args = _build_argparser().parse_args()
    if args.check:
        check()
        return 0
    ingest(
        force=args.force,
        skip_models=args.skip_models,
        export=not args.no_export,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
