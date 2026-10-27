"""Funciones que ejecuta cada tarea del DAG.

Las tareas intercambian datos por archivos parquet en data/staging/<fecha_logica>/ (no por XCom,
porque los volumenes son grandes). Releer los mismos archivos produce siempre el mismo resultado.
"""
from __future__ import annotations

import json
import logging

import pandas as pd

from . import catalog_client, loaders, quality, sources, transform
from .settings import API_CONNECTION_ID, DW_CONNECTION_ID, REQUIRED_VARIABLES, Settings, staging_dir

log = logging.getLogger(__name__)


def _dw_connection():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=DW_CONNECTION_ID).get_conn()


def _in_transaction(work):
    """Ejecuta `work(conn)` en una transaccion: commit si termina bien, rollback si falla."""
    conn = _dw_connection()
    try:
        result = work(conn)
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# --------------------------------------------------------------------------- 1. configuracion
def validate_configuration() -> None:
    """Falla pronto y con mensaje claro si falta una Variable o no hay conexion al DW."""
    settings = Settings.load()
    log.info("Variables OK: %s", {name: "definida" for name in REQUIRED_VARIABLES})
    log.info("Zona horaria de origen: %s | umbral de rechazo: %s", settings.source_timezone, settings.max_rejection_ratio)

    def ping(conn):
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'dw'")
            tables = cur.fetchone()[0]
        if tables < 8:
            raise RuntimeError(f"El esquema dw tiene {tables} tablas; se esperaban al menos 8 (DDL no aplicado?)")
        log.info("Conexion '%s' OK; tablas en dw: %s", DW_CONNECTION_ID, tables)

    _in_transaction(ping)


# --------------------------------------------------------------------------- 2. extraccion
def extract_source(source_system: str, ds: str) -> None:
    settings = Settings.load()
    file_name, dataset = {
        sources.SOURCE_DAILY: (settings.sales_daily_file, settings.kaggle_dataset_sales_daily),
        sources.SOURCE_HISTORY: (settings.sales_history_file, settings.kaggle_dataset_sales_history),
    }[source_system]
    path, origin = sources.resolve_source(source_system, file_name, dataset)
    frame = sources.read_source_csv(path)
    out = staging_dir(ds)
    frame.to_parquet(out / f"{source_system}_raw.parquet", index=False)
    (out / f"{source_system}_origin.json").write_text(json.dumps({"file": path.name, "origin": origin, "rows": len(frame)}))
    log.info("%s: %s filas leidas de %s (%s)", source_system, len(frame), path.name, origin)


def extract_products_catalog(ds: str) -> None:
    """Consume la API de productos. Si esta desactivada o no responde, el pipeline sigue con el respaldo."""
    from airflow.hooks.base import BaseHook

    settings = Settings.load()
    out = staging_dir(ds)
    status = {"source": "keyword_rules", "products": 0}
    if settings.use_products_api:
        try:
            conn = BaseHook.get_connection(API_CONNECTION_ID)
            base_url = f"{conn.schema or 'http'}://{conn.host}:{conn.port}"
            catalog = catalog_client.fetch_catalog(base_url, conn.password or "", settings.api_page_size)
            catalog.to_parquet(out / "products_catalog.parquet", index=False)
            status = {"source": "products_api", "products": len(catalog)}
        except Exception as exc:
            log.warning("Se usara la categorizacion de respaldo (palabras clave): %s", exc)
    else:
        log.info("datamart_use_products_api=false: se usa la categorizacion de respaldo")
    (out / "products_catalog_status.json").write_text(json.dumps(status))


# --------------------------------------------------------------------------- 3. transformacion
def transform_and_unify(ds: str) -> None:
    settings = Settings.load()
    out = staging_dir(ds)

    cleaned, rejections, rows_in = {}, [], {}
    for source in (sources.SOURCE_DAILY, sources.SOURCE_HISTORY):
        raw = pd.read_parquet(out / f"{source}_raw.parquet")
        rows_in[source] = len(raw)
        cleaned[source], rejected = transform.clean_source(
            raw, source, settings.source_timezone, settings.stock_code_pattern
        )
        rejections.append(rejected)
        log.info("%s: %s validas, %s rechazadas", source, len(cleaned[source]), len(rejected))

    unified, cross_rejected = transform.unify_sources(cleaned)
    rejections.append(cross_rejected)
    rejected_all = pd.concat(rejections, ignore_index=True)

    sales, returns = transform.split_sales_and_returns(unified)
    products, variants = transform.build_product_master(unified)

    catalog_file = out / "products_catalog.parquet"
    catalog = pd.read_parquet(catalog_file) if catalog_file.exists() else None
    products = transform.enrich_with_catalog(products, catalog)

    metrics = {
        "rows_in": rows_in,
        "rows_in_total": int(sum(rows_in.values())),
        "sales_rows": len(sales),
        "returns_rows": len(returns),
        "rejected_rows": len(rejected_all),
        "rejected_by_reason": rejected_all.groupby(["source_system", "reject_reason"]).size().rename("n").reset_index().to_dict("records"),
        "products": len(products),
        "anonymous_customer_lines": int(unified["customer_id"].isna().sum()),
        "category_source": products["category_source"].value_counts().to_dict(),
    }
    sales.to_parquet(out / "sales.parquet", index=False)
    returns.to_parquet(out / "returns.parquet", index=False)
    products.to_parquet(out / "products.parquet", index=False)
    variants.to_parquet(out / "variants.parquet", index=False)
    rejected_all.to_parquet(out / "rejected.parquet", index=False)
    (out / "metrics.json").write_text(json.dumps(metrics, default=str))


# --------------------------------------------------------------------------- 4. calidad
def run_quality_checks(ds: str) -> None:
    settings = Settings.load()
    out = staging_dir(ds)
    metrics = json.loads((out / "metrics.json").read_text())
    result = quality.run_quality_checks(
        sales=pd.read_parquet(out / "sales.parquet"),
        returns=pd.read_parquet(out / "returns.parquet"),
        products=pd.read_parquet(out / "products.parquet"),
        rejected=pd.read_parquet(out / "rejected.parquet"),
        rows_in=metrics["rows_in_total"],
        max_rejection_ratio=settings.max_rejection_ratio,
    )
    metrics["quality"] = result
    (out / "metrics.json").write_text(json.dumps(metrics, default=str))
    log.info("Calidad OK: %s", result)


# --------------------------------------------------------------------------- 5. carga
def load_dimensions(ds: str) -> None:
    out = staging_dir(ds)
    result = _in_transaction(lambda conn: loaders.load_dimensions(
        conn,
        pd.read_parquet(out / "sales.parquet"),
        pd.read_parquet(out / "returns.parquet"),
        pd.read_parquet(out / "products.parquet"),
        pd.read_parquet(out / "variants.parquet"),
    ))
    log.info("Dimensiones cargadas: %s", result)


def load_facts(ds: str) -> None:
    out = staging_dir(ds)
    result = _in_transaction(lambda conn: loaders.load_facts(
        conn, pd.read_parquet(out / "sales.parquet"), pd.read_parquet(out / "returns.parquet")
    ))
    log.info("Hechos cargados: %s", result)


def load_rejected_records(ds: str) -> None:
    out = staging_dir(ds)
    result = _in_transaction(lambda conn: loaders.load_rejections(conn, ds, pd.read_parquet(out / "rejected.parquet")))
    log.info("Rechazos registrados: %s", result)


def build_net_revenue(ds: str) -> None:
    result = _in_transaction(loaders.build_net_revenue_aggregate)
    log.info("Agregado neto construido: %s", result)


def verify_warehouse_load(ds: str) -> None:
    out = staging_dir(ds)
    metrics = json.loads((out / "metrics.json").read_text())

    def work(conn):
        result = loaders.verify_load(conn, metrics["sales_rows"], metrics["returns_rows"])
        loaders.record_run_summary(conn, ds, {**metrics, "verification": result})
        return result

    log.info("Verificacion final OK: %s", _in_transaction(work))
