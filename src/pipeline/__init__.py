"""Pipeline medallion del lakehouse de transacciones.

Al importar se fuerza UTF-8 en la salida estándar. Sin esto, la consola de
Windows usa cp1252 y cualquier `print` con un carácter no latino-1 —una flecha,
un signo ± o un emoji en un log— aborta la etapa con `UnicodeEncodeError`
*después* de haber hecho el trabajo. Los runners de CI tienen el mismo problema
cuando la locale no está configurada.
"""

from __future__ import annotations

import contextlib
import sys


def _force_utf8_stdio() -> None:
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        with contextlib.suppress(ValueError, OSError):
            reconfigure(encoding="utf-8", errors="replace")


_force_utf8_stdio()
