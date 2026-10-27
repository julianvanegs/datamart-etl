"""Validaciones de calidad previas a la carga. Si alguna falla, el DAG se detiene y no se carga nada."""
from __future__ import annotations

import pandas as pd

from category_rules import CATEGORIES

from .transform import R_DUP_CROSS


def run_quality_checks(
    sales: pd.DataFrame,
    returns: pd.DataFrame,
    products: pd.DataFrame,
    rejected: pd.DataFrame,
    rows_in: int,
    max_rejection_ratio: float,
) -> dict[str, object]:
    """Devuelve el resultado de cada chequeo; lanza ValueError con todos los incumplimientos."""
    checks: dict[str, bool] = {
        "ventas_con_cantidad_y_precio_positivos": bool(((sales["quantity"] > 0) & (sales["unit_price"] > 0)).all()),
        "devoluciones_con_precio_positivo": bool((returns["unit_price"] > 0).all()),
        "devoluciones_solo_cantidad_no_positiva": bool((returns["quantity"] <= 0).all()),
        "fechas_en_utc_sin_nulos": bool(
            sales["invoice_ts_utc"].notna().all()
            and returns["invoice_ts_utc"].notna().all()
            and str(sales["invoice_ts_utc"].dt.tz) == "UTC"
        ) if len(sales) else True,
        "codigos_de_producto_normalizados": bool(
            pd.concat([sales["stock_code"], returns["stock_code"]]).map(lambda c: c == c.upper() and " " not in c).all()
        ),
        "line_id_unico_entre_ventas_y_devoluciones": bool(
            pd.concat([sales["line_id"], returns["line_id"]]).is_unique
        ),
        "todo_codigo_tiene_producto": bool(
            set(pd.concat([sales["stock_code"], returns["stock_code"]])) <= set(products["stock_code"])
        ),
        "producto_unico_por_codigo": bool(products["stock_code"].is_unique),
        "categorias_validas": bool(products["category"].isin(CATEGORIES).all()),
        "cuadre_entradas_vs_salidas": rows_in == len(sales) + len(returns) + len(rejected),
    }
    # El solapamiento entre fuentes es esperado por diseno (el historico repite parte del diario);
    # el umbral mide solo problemas de calidad, no ese descarte controlado.
    quality_rejections = int((rejected["reject_reason"] != R_DUP_CROSS).sum())
    ratio = quality_rejections / rows_in if rows_in else 0.0
    checks["tasa_de_rechazo_bajo_el_umbral"] = ratio <= max_rejection_ratio

    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        raise ValueError(f"Fallaron chequeos de calidad: {failed} (rechazo={ratio:.2%}, umbral={max_rejection_ratio:.0%})")
    return {"checks": checks, "rejection_ratio": round(ratio, 6)}
