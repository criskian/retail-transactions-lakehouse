"""Descarga winutils.exe y hadoop.dll para Hadoop en Windows.
Ambos son requeridos por PySpark para escribir archivos locales:
  - winutils.exe: comandos de permisos POSIX emulados
  - hadoop.dll  : implementa NativeIO$Windows.access0 (lectura/listado de archivos)

Uso:
    python scripts/setup_winutils.py
"""

import shutil
import sys
import urllib.request
from pathlib import Path

HADOOP_HOME = Path("C:/hadoop")
WINUTILS_DIR = HADOOP_HOME / "bin"

# cdarlint/winutils es el repositorio estándar para binarios Hadoop en Windows.
# PySpark 3.5.5 trae Hadoop 3.3.4 — la hadoop.dll DEBE coincidir con esa versión
# o NativeIO$Windows.access0 lanza UnsatisfiedLinkError. 3.3.4 va primero.
VERSIONS = ["3.3.4", "3.3.5", "3.3.6", "3.3.2", "3.3.1", "3.3.0"]
BASE_URL = "https://github.com/cdarlint/winutils/raw/master/hadoop-{}/bin/{}"

FILES = ["winutils.exe", "hadoop.dll"]


def _download_file(filename: str) -> bool:
    target = WINUTILS_DIR / filename
    if target.exists() and target.stat().st_size > 0:
        print(f"[ok] {filename} ya existe en {target}")
        return True

    for version in VERSIONS:
        url = BASE_URL.format(version, filename)
        print(f"  {filename}: intentando Hadoop {version} ... ", end="", flush=True)
        try:
            urllib.request.urlretrieve(url, target)
            size_kb = target.stat().st_size // 1024
            print(f"OK ({size_kb} KB)")
            return True
        except Exception as e:
            print(f"fallo ({e})")
    return False


def download():
    WINUTILS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Destino: {WINUTILS_DIR}\n")

    ok = all(_download_file(f) for f in FILES)
    if not ok:
        print("\nERROR: No se pudieron descargar los binarios automaticamente.")
        print("Descargalos manualmente desde https://github.com/cdarlint/winutils")
        print(f"y coloca winutils.exe y hadoop.dll en: {WINUTILS_DIR}")
        sys.exit(1)

    # hadoop.dll también debe estar en System32 para que el JVM lo cargue
    # de forma fiable (System.loadLibrary busca ahí). Intentamos copiarlo;
    # si falla por permisos, el PATH configurado en spark_session.py es el respaldo.
    try:
        system32 = Path("C:/Windows/System32/hadoop.dll")
        if not system32.exists():
            shutil.copy2(WINUTILS_DIR / "hadoop.dll", system32)
            print(f"\n[ok] hadoop.dll copiado a {system32}")
    except PermissionError:
        print("\n[aviso] No se pudo copiar hadoop.dll a System32 (sin permisos admin).")
        print("        No pasa nada: spark_session.py agrega C:\\hadoop\\bin al PATH.")


if __name__ == "__main__":
    download()
    print(f"\n[listo] HADOOP_HOME = {HADOOP_HOME}")
    print("\nSiguiente paso:")
    print("  .venv\\Scripts\\python -m src.pipeline.run --step all")
