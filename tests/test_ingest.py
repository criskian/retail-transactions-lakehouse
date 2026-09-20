"""Detección de cambios en el landing."""

from __future__ import annotations

import json

from src.pipeline.ingest import diff


def test_diff_clasifica_nuevos_cambiados_y_eliminados():
    previo = {"Transactions/102_Tran.csv": "aaa", "Products/Categories.csv": "bbb"}
    actual = {"Transactions/102_Tran.csv": "ZZZ", "Transactions/999_Tran.csv": "ccc"}

    nuevos, cambiados, eliminados = diff(actual, previo)

    assert nuevos == ["Transactions/999_Tran.csv"]
    assert cambiados == ["Transactions/102_Tran.csv"]
    assert eliminados == ["Products/Categories.csv"]


def test_diff_sin_novedades():
    m = {"Transactions/102_Tran.csv": "aaa"}
    assert diff(m, m) == ([], [], [])


def test_manifest_normaliza_separadores_de_windows(tmp_path, monkeypatch):
    """Un manifest escrito en Windows no debe marcar todo como nuevo en Linux.

    Antes las claves se guardaban con el separador del sistema, así que un
    manifest generado en Windows (`Transactions\\102_Tran.csv`) no casaba con
    las rutas POSIX y el pipeline reprocesaba siempre.
    """
    from src.pipeline import ingest

    manifest = tmp_path / "_manifest.json"
    manifest.write_text(json.dumps({"Transactions\\102_Tran.csv": "aaa"}), encoding="utf-8")
    monkeypatch.setattr(ingest, "MANIFEST", manifest)

    cargado = ingest._load_manifest()
    assert cargado == {"Transactions/102_Tran.csv": "aaa"}
    assert diff({"Transactions/102_Tran.csv": "aaa"}, cargado) == ([], [], [])


def test_lock_impide_dos_pipelines_simultaneos(tmp_path, monkeypatch):
    import pytest

    from src.pipeline import ingest

    monkeypatch.setattr(ingest, "LOCK", tmp_path / "_pipeline.lock")
    with ingest._pipeline_lock():
        assert ingest.LOCK.exists()
        with pytest.raises(RuntimeError, match="pipeline en ejecución"), ingest._pipeline_lock():
            pass
    assert not ingest.LOCK.exists()


def test_lock_obsoleto_se_descarta(tmp_path, monkeypatch):
    import os
    import time

    from src.pipeline import ingest

    lock = tmp_path / "_pipeline.lock"
    lock.write_text("pid=1", encoding="utf-8")
    viejo = time.time() - (ingest.LOCK_STALE_SECONDS + 60)
    os.utime(lock, (viejo, viejo))
    monkeypatch.setattr(ingest, "LOCK", lock)

    with ingest._pipeline_lock():
        pass  # no debe lanzar
