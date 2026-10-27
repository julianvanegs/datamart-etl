"""Construye el catalogo de productos del 'sistema legado' a partir de los CSV disponibles."""
from __future__ import annotations

import os
import re
from pathlib import Path

import pandas as pd

from category_rules import classify_description, supplier_country_for

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
SEEDS_DIR = Path(os.environ.get("SEEDS_DIR", "/seeds"))
VALID_CODE = re.compile(r"^(\d{5}[A-Z]{0,3}|DCGS\d{4}[A-Z]?)$")
# Un producto sin ventas en los ultimos N dias del periodo cubierto se considera descontinuado
DISCONTINUED_AFTER_DAYS = 90

# (alias posibles de columna) -> nombre interno
_ALIASES = {
    "stock_code": ("stockcode", "stock_code"),
    "description": ("description",),
    "invoice_date": ("invoicedate", "invoice_date"),
}


def _find_sources() -> list[Path]:
    raw = DATA_DIR / "raw"
    files = sorted(raw.glob("*.csv")) if raw.exists() else []
    return files or sorted(SEEDS_DIR.glob("*.csv"))


def sources_signature() -> tuple:
    """Huella de los archivos fuente: si cambia, el catalogo se vuelve a construir."""
    return tuple((p.name, p.stat().st_size, int(p.stat().st_mtime)) for p in _find_sources())


def _read(path: Path) -> pd.DataFrame:
    # data.csv de Kaggle viene en ISO-8859-1: se prueba UTF-8 y, si falla al leer todo el archivo, Latin-1.
    for encoding in ("utf-8", "latin-1"):
        try:
            header = pd.read_csv(path, nrows=0, encoding=encoding)
            normalized = {c: re.sub(r"[^a-z_]", "", c.strip().lower().replace(" ", "_")) for c in header.columns}
            pick = {}
            for internal, aliases in _ALIASES.items():
                for original, norm in normalized.items():
                    if norm in aliases:
                        pick[internal] = original
            if len(pick) < 3:
                return pd.DataFrame(columns=list(_ALIASES))
            frame = pd.read_csv(path, usecols=list(pick.values()), dtype=str, encoding=encoding)
            return frame.rename(columns={v: k for k, v in pick.items()})
        except UnicodeDecodeError:
            continue
    return pd.DataFrame(columns=list(_ALIASES))


def build_catalog() -> list[dict]:
    frames = [_read(p) for p in _find_sources()]
    data = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=list(_ALIASES))
    data["stock_code"] = data["stock_code"].fillna("").str.upper().str.replace(r"\s+", "", regex=True)
    data = data[data["stock_code"].map(lambda c: bool(VALID_CODE.match(c)))]
    data["description"] = data["description"].fillna("").str.strip().str.replace(r"\s+", " ", regex=True).str.upper()
    data["invoice_date"] = pd.to_datetime(data["invoice_date"], errors="coerce", format="mixed")

    last_overall = data["invoice_date"].max()
    products: list[dict] = []
    for code, group in data.groupby("stock_code", sort=True):
        descriptions = group["description"][group["description"] != ""]
        if descriptions.empty:
            name = "SIN DESCRIPCION"
        else:
            counts = descriptions.value_counts()
            top = counts[counts == counts.max()].index
            name = sorted(top, key=lambda d: (-len(d), d))[0]
        last_seen = group["invoice_date"].max()
        active = bool(pd.notna(last_seen) and pd.notna(last_overall)
                      and (last_overall - last_seen).days <= DISCONTINUED_AFTER_DAYS)
        products.append({
            "code": code,
            "name": name,
            "category": classify_description(name),
            "supplier_country": supplier_country_for(code),
            "is_active": active,
        })
    return products
