"""Construcción de la SparkSession y saneamiento del entorno.

Este módulo resuelve tres incompatibilidades que, si no se corrigen *antes* de
levantar el JVM, hacen que el pipeline falle de formas poco obvias:

1. **Java 21+** — PySpark 3.5.x soporta oficialmente hasta Java 17. Con Java 21
   los Python workers crashean (`WinError 10038` / `EOFException`) al
   materializar DataFrames. Si hay un JDK 17 instalado lo forzamos vía
   ``JAVA_HOME``.

2. **`python3` inexistente en Windows** — Spark lanza los Python workers
   invocando ``python3`` por defecto. En Windows ese ejecutable no existe y
   cualquier etapa que necesite un worker (p. ej. ``spark.createDataFrame``
   sobre una colección Python) muere con
   ``java.io.IOException: Cannot run program "python3"``. Se fija
   ``PYSPARK_PYTHON`` al intérprete actual.

3. **Hadoop native IO en Windows** — sin ``winutils.exe`` + ``hadoop.dll`` el
   listado de archivos lanza ``UnsatisfiedLinkError: NativeIO$Windows.access0``
   dentro de un ForkJoinPool, donde la excepción se traga y el job **se cuelga
   indefinidamente** en lugar de fallar. Ver ``scripts/setup_winutils.py``.
"""

from __future__ import annotations

import os
import platform
import re
import subprocess
import sys
from pathlib import Path

from pyspark.sql import SparkSession

HADOOP_HOME = Path(os.environ.get("HADOOP_HOME", "C:/hadoop"))
MAX_SUPPORTED_JAVA = 17


# ---------------------------------------------------------------------------
# Python workers
# ---------------------------------------------------------------------------
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


# ---------------------------------------------------------------------------
# Java
# ---------------------------------------------------------------------------
def _java_major(java_home: str) -> int | None:
    """Devuelve la versión mayor del JDK en `java_home`, o None si no se puede leer."""
    java_bin = Path(java_home) / "bin" / ("java.exe" if os.name == "nt" else "java")
    if not java_bin.exists():
        return None
    try:
        out = subprocess.run(
            [str(java_bin), "-version"], capture_output=True, text=True, timeout=15
        )
    except (OSError, subprocess.SubprocessError):
        return None
    # `java -version` escribe en stderr: 'openjdk version "17.0.18" ...'
    m = re.search(r'version "(\d+)', (out.stderr or "") + (out.stdout or ""))
    if not m:
        return None
    major = int(m.group(1))
    return 8 if major == 1 else major


def _find_supported_jdk() -> str | None:
    """Busca un JDK <= MAX_SUPPORTED_JAVA en las rutas habituales de instalación."""
    candidates = [
        Path("C:/Program Files/Eclipse Adoptium"),
        Path("C:/Program Files/Java"),
        Path("C:/Program Files/Microsoft"),
        Path(os.environ.get("LOCALAPPDATA", "")) / "Programs" / "Eclipse Adoptium",
        Path("/usr/lib/jvm"),
        Path("/Library/Java/JavaVirtualMachines"),
    ]
    for base in candidates:
        try:
            if not base.exists():
                continue
            entries = sorted(base.iterdir(), reverse=True)
        except OSError:
            continue
        for d in entries:
            # En macOS el JDK vive en <bundle>/Contents/Home
            for home in (d, d / "Contents" / "Home"):
                major = _java_major(str(home))
                if major is not None and major <= MAX_SUPPORTED_JAVA:
                    return str(home)
    return None


def _ensure_java() -> None:
    current = os.environ.get("JAVA_HOME", "")
    if current and (major := _java_major(current)) is not None and major <= MAX_SUPPORTED_JAVA:
        return

    found = _find_supported_jdk()
    if found is None:
        current_desc = f"{current} (Java {_java_major(current)})" if current else "no definido"
        raise RuntimeError(
            f"PySpark 3.5 requiere Java <= {MAX_SUPPORTED_JAVA}, pero JAVA_HOME es {current_desc} "
            f"y no se encontró ningún JDK compatible instalado.\n"
            f"Instala Temurin {MAX_SUPPORTED_JAVA} (https://adoptium.net/) o exporta "
            f"JAVA_HOME apuntando a uno."
        )

    os.environ["JAVA_HOME"] = found
    java_bin = str(Path(found) / "bin")
    if java_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")
    print(f"[spark] JAVA_HOME ajustado a un JDK compatible: {found}")


# ---------------------------------------------------------------------------
# Hadoop native IO (solo Windows)
# ---------------------------------------------------------------------------
def _ensure_hadoop_native() -> None:
    if os.name != "nt":
        return

    hadoop_bin = HADOOP_HOME / "bin"
    missing = [f for f in ("winutils.exe", "hadoop.dll") if not (hadoop_bin / f).exists()]
    if missing:
        raise RuntimeError(
            f"Faltan los binarios nativos de Hadoop en {hadoop_bin}: {', '.join(missing)}.\n"
            f"Sin ellos Spark no puede listar archivos locales en Windows y el job se cuelga "
            f"silenciosamente (UnsatisfiedLinkError dentro de un ForkJoinPool).\n"
            f"Solución:  python scripts/setup_winutils.py"
        )

    os.environ.setdefault("HADOOP_HOME", str(HADOOP_HOME))
    if str(hadoop_bin) not in os.environ.get("PATH", ""):
        os.environ["PATH"] = str(hadoop_bin) + os.pathsep + os.environ.get("PATH", "")

    # `hadoop.dll` implementa NativeIO$Windows.access0 y el JVM lo carga desde
    # java.library.path. En modo local `spark.driver.extraJavaOptions` llega
    # tarde (el JVM ya arrancó cuando Spark lee ese config), así que la única
    # vía fiable es PYSPARK_SUBMIT_ARGS. Usamos forward-slashes para que shlex
    # no interprete los backslashes como escapes.
    lib_path = str(hadoop_bin).replace("\\", "/")
    existing = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
    if "java.library.path" not in existing:
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--driver-java-options -Djava.library.path={lib_path} pyspark-shell"
        )


def prepare_environment() -> None:
    """Sanea el entorno. Idempotente; se invoca automáticamente en `get_spark`."""
    _ensure_java()
    _ensure_hadoop_native()


# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------
def get_spark(app_name: str = "supermarket-pipeline") -> SparkSession:
    prepare_environment()

    driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", "8g")
    shuffle_partitions = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8")

    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.driver.memory", driver_memory)
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


def describe_environment() -> dict[str, str]:
    """Snapshot del entorno resuelto — útil para logs y para el panel de diagnóstico."""
    return {
        "python": sys.version.split()[0],
        "platform": f"{platform.system()} {platform.release()}",
        "java_home": os.environ.get("JAVA_HOME", ""),
        "java_major": str(_java_major(os.environ.get("JAVA_HOME", "")) or "?"),
        "hadoop_home": os.environ.get("HADOOP_HOME", ""),
        "pyspark_python": os.environ.get("PYSPARK_PYTHON", ""),
    }
