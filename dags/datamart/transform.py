"""Limpieza, reglas de negocio y unificacion de las fuentes.

Cada decision de negocio esta en una funcion o constante con nombre propio, para que
la logica sea legible. Todas las funciones son puras (DataFrame entra, DataFrame sale)
y por eso se pueden probar sin Airflow ni base de datos. Ver docs/DECISIONES_TECNICAS.md.
"""
from __future__ import annotations

import hashlib
import json
import re
import uuid

import numpy as np
import pandas as pd

from .sources import DATE_FORMATS, SOURCE_DAILY, SOURCE_HISTORY

# ---------------------------------------------------------------------------
# Motivos de rechazo (valores que se guardan en rejected_records.reject_reason)
# ---------------------------------------------------------------------------
R_MISSING_INVOICE = "missing_invoice_no"
R_MISSING_STOCK = "missing_stock_code"
R_BAD_QUANTITY = "invalid_quantity"
R_BAD_PRICE = "invalid_unit_price"
R_BAD_DATE = "invalid_invoice_date"
R_NON_CATALOG = "non_catalog_stock_code"
R_PRICE_SALE = "non_positive_unit_price_sale"
R_PRICE_RETURN = "non_positive_unit_price_return_or_adjustment"
R_DUP_SOURCE = "duplicate_within_source"
R_DUP_CROSS = "duplicate_across_sources_history_overlap"

# Prioridad entre fuentes cuando una misma linea aparece en ambas:
# gana el volcado diario operacional (sales_daily); la linea de la historica se rechaza.
SOURCE_PRIORITY = (SOURCE_DAILY, SOURCE_HISTORY)

# Nombres de pais distintos para el mismo pais en los datasets de origen
COUNTRY_ALIASES = {
    "EIRE": "Ireland",
    "RSA": "South Africa",
    "USA": "United States",
    "UK": "United Kingdom",
    "UNSPECIFIED": "Unspecified",
}

NO_DESCRIPTION = "SIN DESCRIPCION"

# Columnas con las que se detecta una linea duplicada dentro de una misma fuente
_WITHIN_SOURCE_KEY = ["invoice_no", "stock_code", "invoice_ts_utc", "quantity", "unit_price", "customer_id", "country"]
# Columnas con las que se detecta la misma linea entre las dos fuentes (sin cliente ni pais,
# porque son los campos que mas varian de formato entre datasets)
_CROSS_SOURCE_KEY = ["invoice_no", "stock_code", "invoice_ts_utc", "quantity", "unit_price"]


# ---------------------------------------------------------------------------
# Normalizaciones de campo
# ---------------------------------------------------------------------------
def normalize_stock_code(series: pd.Series) -> pd.Series:
    """Codigos en MAYUSCULAS y sin espacios (regla del negocio)."""
    return series.fillna("").str.upper().str.replace(r"\s+", "", regex=True)


def normalize_description(series: pd.Series) -> pd.Series:
    """Quita espacios sobrantes (conserva mayusculas/minusculas originales)."""
    cleaned = series.fillna("").str.strip().str.replace(r"\s+", " ", regex=True)
    return cleaned.where(cleaned != "", None)


def normalize_customer_id(series: pd.Series) -> pd.Series:
    """'17850.0' -> '17850'; vacio -> None (cliente no identificado)."""
    stripped = series.fillna("").str.strip()
    numeric = pd.to_numeric(stripped, errors="coerce")
    as_int = numeric.dropna().astype("int64").astype(str)
    result = stripped.copy()
    result.loc[as_int.index] = as_int
    return result.where(result != "", None)


def normalize_country(series: pd.Series) -> pd.Series:
    stripped = series.fillna("").str.strip()
    stripped = stripped.where(stripped != "", "Unspecified")
    return stripped.map(lambda c: COUNTRY_ALIASES.get(c.upper(), c))


def to_utc(local_naive: pd.Series, source_timezone: str) -> pd.Series:
    """Fecha local (sin zona) -> UTC.

    Horas ambiguas del cambio de horario (otono) se interpretan como hora estandar;
    horas inexistentes (primavera) se adelantan una hora.
    """
    return (
        local_naive.dt.tz_localize(
            source_timezone,
            ambiguous=np.zeros(len(local_naive), dtype=bool),
            nonexistent="shift_forward",
        )
        .dt.tz_convert("UTC")
    )


def parse_local_dates(series: pd.Series, source_system: str) -> pd.Series:
    """Parsea con los formatos conocidos de la fuente; lo que no calce se reintenta de forma flexible."""
    parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    for fmt in DATE_FORMATS[source_system]:
        pending = parsed.isna() & series.notna()
        if not pending.any():
            break
        parsed.loc[pending] = pd.to_datetime(series[pending], format=fmt, errors="coerce")
    pending = parsed.isna() & series.notna()
    if pending.any():
        parsed.loc[pending] = pd.to_datetime(series[pending], format="mixed", errors="coerce", dayfirst=False)
    return parsed


def make_line_id(frame: pd.DataFrame) -> pd.Series:
    """UUID determinista de la linea: la misma linea siempre produce el mismo id (clave de idempotencia)."""
    key = (
        frame["invoice_no"] + "|" + frame["stock_code"] + "|"
        + frame["invoice_ts_utc"].dt.strftime("%Y-%m-%dT%H:%M:%S") + "|"
        + frame["quantity"].astype(str) + "|"
        + frame["unit_price"].round(4).map("{:.4f}".format) + "|"
        + frame["customer_id"].fillna("") + "|" + frame["country"]
    )
    return key.map(lambda text: str(uuid.UUID(hashlib.md5(text.encode("utf-8")).hexdigest())))


def _payload(rows: pd.DataFrame) -> list[str]:
    records = rows.astype(object).where(rows.notna(), None).to_dict("records")
    return [json.dumps(r, ensure_ascii=False, default=str) for r in records]


# ---------------------------------------------------------------------------
# Limpieza de UNA fuente
# ---------------------------------------------------------------------------
def clean_source(
    raw: pd.DataFrame,
    source_system: str,
    source_timezone: str,
    stock_code_pattern: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Aplica las reglas de calidad a una fuente.

    Devuelve (lineas_validas, lineas_rechazadas). Cada linea cae en exactamente uno de los dos
    conjuntos: valida o rechazada con UN motivo (el primero que incumple, en el orden de abajo).
    """
    df = raw.copy()
    reason = pd.Series(pd.NA, index=df.index, dtype="object")

    def reject_where(mask: pd.Series, text: str) -> None:
        reason.loc[mask.fillna(False) & reason.isna()] = text

    # --- normalizaciones ---------------------------------------------------
    df["invoice_no"] = df["invoice_no"].fillna("").str.strip().str.upper()
    df["stock_code_raw"] = df["stock_code"]
    df["stock_code"] = normalize_stock_code(df["stock_code"])
    df["description"] = normalize_description(df["description"])
    df["customer_id"] = normalize_customer_id(df["customer_id"])
    df["country"] = normalize_country(df["country"])
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["invoice_ts_utc"] = to_utc(parse_local_dates(df["invoice_date"], source_system), source_timezone)

    # --- reglas de rechazo (el orden define el motivo registrado) -----------
    reject_where(df["invoice_no"] == "", R_MISSING_INVOICE)
    reject_where(df["stock_code"] == "", R_MISSING_STOCK)
    reject_where(df["quantity"].isna(), R_BAD_QUANTITY)
    reject_where(df["unit_price"].isna(), R_BAD_PRICE)
    reject_where(df["invoice_ts_utc"].isna(), R_BAD_DATE)
    # Codigos que no siguen el patron del catalogo (POST, D, M, BANK CHARGES, gift_*, ...)
    # son servicios, comisiones o ajustes: no son productos y no pueden cruzarse con el catalogo.
    reject_where(~df["stock_code"].str.match(stock_code_pattern), R_NON_CATALOG)
    # Precio <= 0: en ventas es invalido (regla del negocio); en devoluciones/ajustes no permite
    # valorar el monto, por eso tambien se rechaza con un motivo distinto.
    reject_where((df["unit_price"] <= 0) & (df["quantity"] > 0), R_PRICE_SALE)
    reject_where(df["unit_price"] <= 0, R_PRICE_RETURN)

    # --- duplicados exactos dentro de la fuente (se conserva la primera aparicion) -------
    candidate = df[reason.isna()]
    dup_mask = candidate.duplicated(subset=_WITHIN_SOURCE_KEY, keep="first")
    reject_where(dup_mask.reindex(df.index, fill_value=False), R_DUP_SOURCE)

    # --- separar validas y rechazadas -----------------------------------------
    rejected = pd.DataFrame({
        "source_system": source_system,
        "source_row": df.loc[reason.notna(), "source_row"],
        "reject_reason": reason[reason.notna()],
        "invoice_no_raw": raw.loc[reason.notna(), "invoice_no"],
        "stock_code_raw": df.loc[reason.notna(), "stock_code_raw"],
        "payload": _payload(raw.loc[reason.notna()]),
    }).reset_index(drop=True)

    clean = df[reason.isna()].copy()
    clean["quantity"] = clean["quantity"].astype("int64")
    clean["unit_price"] = clean["unit_price"].astype("float64")
    clean["desc_norm"] = clean["description"].str.upper()
    clean["is_cancellation"] = clean["invoice_no"].str.startswith("C")
    clean["source_system"] = source_system
    clean["line_id"] = make_line_id(clean) if len(clean) else pd.Series(dtype="object")
    clean = clean[[
        "line_id", "source_system", "source_row", "invoice_no", "invoice_ts_utc", "stock_code",
        "description", "desc_norm", "quantity", "unit_price", "customer_id", "country", "is_cancellation",
    ]].reset_index(drop=True)
    return clean, rejected


# ---------------------------------------------------------------------------
# Union de fuentes
# ---------------------------------------------------------------------------
def unify_sources(frames: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Une las fuentes aplicando la prioridad (sales_daily > sales_history).

    Una linea de la fuente de menor prioridad se considera duplicado real cuando coincide con una de
    mayor prioridad en la clave compuesta (factura, producto, fecha UTC, cantidad, precio).
    """
    kept: list[pd.DataFrame] = []
    rejected: list[pd.DataFrame] = []
    seen_keys: pd.MultiIndex | None = None

    for source in SOURCE_PRIORITY:
        frame = frames[source]
        if seen_keys is not None and len(frame):
            keys = pd.MultiIndex.from_frame(frame[_CROSS_SOURCE_KEY])
            overlap = keys.isin(seen_keys)
            dup = frame[overlap]
            if len(dup):
                rejected.append(pd.DataFrame({
                    "source_system": source,
                    "source_row": dup["source_row"].to_numpy(),
                    "reject_reason": R_DUP_CROSS,
                    "invoice_no_raw": dup["invoice_no"].to_numpy(),
                    "stock_code_raw": dup["stock_code"].to_numpy(),
                    "payload": _payload(dup),
                }))
            frame = frame[~overlap]
        kept.append(frame)
        part = pd.MultiIndex.from_frame(frame[_CROSS_SOURCE_KEY])
        seen_keys = part if seen_keys is None else seen_keys.append(part)

    unified = pd.concat(kept, ignore_index=True)
    # Dos lineas con la misma clave compuesta pero distinto cliente/pais compartirian line_id solo si fueran
    # identicas, y eso ya se descarto; esta verificacion es una red de seguridad.
    unified = unified.drop_duplicates(subset="line_id", keep="first").reset_index(drop=True)
    rej = pd.concat(rejected, ignore_index=True) if rejected else _empty_rejections()
    return unified, rej


def _empty_rejections() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "source_system", "source_row", "reject_reason", "invoice_no_raw", "stock_code_raw", "payload",
    ])


def split_sales_and_returns(unified: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Regla del negocio: cantidad <= 0 es devolucion/ajuste; cantidad > 0 es venta."""
    sales = unified[unified["quantity"] > 0].copy()
    sales["gross_revenue"] = (sales["quantity"] * sales["unit_price"]).round(4)

    returns = unified[unified["quantity"] <= 0].copy()
    returns["quantity_returned"] = returns["quantity"].abs()
    returns["return_amount"] = (returns["quantity_returned"] * returns["unit_price"]).round(4)
    return sales.reset_index(drop=True), returns.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Maestro de productos
# ---------------------------------------------------------------------------
def build_product_master(unified: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Una fila por codigo de producto + tabla de variantes de descripcion.

    Descripcion canonica = la mas frecuente (ignorando mayusculas/minusculas y espacios).
    Si hay empate gana la mas larga (suele ser la mas informativa) y luego el orden alfabetico.
    """
    with_desc = unified[unified["desc_norm"].notna()]
    counts = (
        with_desc.groupby(["stock_code", "desc_norm"]).size().rename("n").reset_index()
        .assign(length=lambda d: d["desc_norm"].str.len())
        .sort_values(["stock_code", "n", "length", "desc_norm"], ascending=[True, False, False, True])
    )
    canonical = counts.drop_duplicates("stock_code")[["stock_code", "desc_norm"]].rename(
        columns={"desc_norm": "canonical_description"}
    )

    stats = unified.groupby("stock_code").agg(
        raw_variant_count=("description", "nunique"),
        normalized_variant_count=("desc_norm", "nunique"),
        first_seen_utc=("invoice_ts_utc", "min"),
        last_seen_utc=("invoice_ts_utc", "max"),
    ).reset_index()

    products = stats.merge(canonical, on="stock_code", how="left")
    products["canonical_description"] = products["canonical_description"].fillna(NO_DESCRIPTION)

    variants = (
        unified[unified["description"].notna()]
        .groupby(["stock_code", "description"]).size().rename("occurrences").reset_index()
        .rename(columns={"description": "raw_description"})
    )
    return products, variants


def enrich_with_catalog(products: pd.DataFrame, catalog: pd.DataFrame | None) -> pd.DataFrame:
    """Asigna categoria. Con API: la categoria del catalogo. Sin API (o producto ausente): reglas por palabras clave."""
    from category_rules import CATEGORIES, classify_description  # modulo compartido con la API

    result = products.copy()
    if catalog is not None and len(catalog):
        cat = catalog.rename(columns={"code": "stock_code", "name": "catalog_name"})
        cat["stock_code"] = normalize_stock_code(cat["stock_code"])
        result = result.merge(
            cat[["stock_code", "category", "supplier_country", "is_active"]].drop_duplicates("stock_code"),
            on="stock_code", how="left",
        )
    else:
        result["category"] = None
        result["supplier_country"] = None
        result["is_active"] = None

    from_api = result["category"].isin(CATEGORIES)
    result["category_source"] = np.where(from_api, "products_api", "keyword_rules")
    fallback = result.loc[~from_api, "canonical_description"].map(classify_description)
    result.loc[~from_api, "category"] = fallback
    return result
