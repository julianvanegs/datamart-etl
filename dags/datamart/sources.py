"""Localizacion y lectura de las fuentes CSV.

Orden de busqueda de cada archivo:
  1. data/raw/<archivo>          (CSV reales de Kaggle colocados por el usuario)
  2. descarga desde Kaggle       (solo si hay KAGGLE_USERNAME y KAGGLE_KEY)
  3. seeds/<archivo>_sample.csv  (datos de prueba incluidos en el repositorio)
"""
from __future__ import annotations

import logging
import os
import re
import subprocess
from pathlib import Path

import pandas as pd

from .settings import DATA_DIR, SEEDS_DIR

log = logging.getLogger(__name__)

SOURCE_DAILY = "sales_daily"
SOURCE_HISTORY = "sales_history"

SEED_FILES = {
    SOURCE_DAILY: "sales_daily_sample.csv",
    SOURCE_HISTORY: "sales_history_sample.csv",
}

# Formato de fecha esperado en cada fuente (hora local de Londres, sin zona horaria)
DATE_FORMATS = {
    SOURCE_DAILY: "%m/%d/%Y %H:%M",      # 12/1/2010 8:26
    SOURCE_HISTORY: "%Y-%m-%d %H:%M:%S",  # 2009-12-01 07:45:00
}

# Los dos datasets nombran distinto las mismas columnas: se unifican aqui.
COLUMN_ALIASES = {
    "invoice_no": ("invoiceno", "invoice"),
    "stock_code": ("stockcode",),
    "description": ("description",),
    "quantity": ("quantity",),
    "invoice_date": ("invoicedate",),
    "unit_price": ("unitprice", "price"),
    "customer_id": ("customerid",),
    "country": ("country",),
}
STANDARD_COLUMNS = list(COLUMN_ALIASES)


def _kaggle_available() -> bool:
    return bool(os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY"))


def _download_from_kaggle(dataset: str, target_dir: Path) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    log.info("Descargando %s desde Kaggle...", dataset)
    subprocess.run(
        ["kaggle", "datasets", "download", "-d", dataset, "-p", str(target_dir), "--unzip"],
        check=True,
    )


def resolve_source(source_system: str, file_name: str, kaggle_dataset: str) -> tuple[Path, str]:
    """Devuelve (ruta_del_csv, origen) donde origen es 'raw' | 'kaggle' | 'seed'."""
    raw_dir = DATA_DIR / "raw"
    candidate = raw_dir / file_name
    if candidate.exists():
        return candidate, "raw"

    if _kaggle_available():
        try:
            _download_from_kaggle(kaggle_dataset, raw_dir)
            if candidate.exists():
                return candidate, "kaggle"
            # El dataset puede traer el CSV con otro nombre: se toma el CSV nuevo mas grande.
            csvs = sorted(raw_dir.glob("*.csv"), key=lambda p: p.stat().st_size, reverse=True)
            if csvs:
                log.warning("%s no existe; se usa %s", file_name, csvs[0].name)
                return csvs[0], "kaggle"
        except Exception as exc:  # sin red, credenciales invalidas, etc.
            log.warning("Descarga de Kaggle fallida (%s). Se usara el seed.", exc)

    seed = SEEDS_DIR / SEED_FILES[source_system]
    if not seed.exists():
        raise FileNotFoundError(f"No hay datos para {source_system}: ni {candidate} ni {seed}")
    log.warning("Usando datos de prueba %s para %s (coloca los CSV reales en data/raw/).", seed.name, source_system)
    return seed, "seed"


def _canonical(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def read_source_csv(path: Path) -> pd.DataFrame:
    """Lee el CSV como texto (sin inferir tipos) y renombra a columnas estandar.

    Se agrega `source_row`: numero de fila en el archivo original (el encabezado es la fila 1),
    para poder rastrear cualquier registro rechazado hasta su origen.
    """
    last_error: Exception | None = None
    for encoding in ("utf-8", "latin-1"):  # data.csv de Kaggle viene en ISO-8859-1
        try:
            raw = pd.read_csv(path, dtype=str, encoding=encoding, keep_default_na=False, na_values=[""])
            break
        except UnicodeDecodeError as exc:
            last_error = exc
    else:
        raise last_error  # type: ignore[misc]

    by_canonical = {_canonical(c): c for c in raw.columns}
    rename, missing = {}, []
    for standard, aliases in COLUMN_ALIASES.items():
        found = next((by_canonical[a] for a in aliases if a in by_canonical), None)
        if found is None:
            missing.append(standard)
        else:
            rename[found] = standard
    if missing:
        raise ValueError(f"{path.name}: faltan columnas {missing}. Encontradas: {list(raw.columns)}")

    frame = raw.rename(columns=rename)[STANDARD_COLUMNS].copy()
    frame.insert(0, "source_row", range(2, len(frame) + 2))
    return frame
