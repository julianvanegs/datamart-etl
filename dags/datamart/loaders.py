"""Carga al repositorio analitico (PostgreSQL).

Todas las funciones reciben una conexion psycopg2 y NO hacen commit: la tarea que las llama
decide la transaccion. Asi cada tarea del DAG es atomica (o carga todo, o no carga nada).

Idempotencia (ver docs/DECISIONES_TECNICAS.md):
  * dimensiones y hechos  -> INSERT ... ON CONFLICT DO UPDATE sobre claves naturales / line_id
  * log de rechazos       -> DELETE de la fecha logica + INSERT
  * agregado neto         -> TRUNCATE + INSERT recalculado desde los hechos
  * resumen de ejecucion  -> UPSERT por fecha logica
"""
from __future__ import annotations

import io
import json

import pandas as pd

SCHEMA = "dw"


def _copy_frame(cur, frame: pd.DataFrame, table: str, columns: list[str]) -> None:
    buffer = io.StringIO()
    frame[columns].to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)
    cur.copy_expert(
        f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buffer
    )


# ---------------------------------------------------------------------------
# Dimensiones
# ---------------------------------------------------------------------------
def load_dimensions(conn, sales: pd.DataFrame, returns: pd.DataFrame, products: pd.DataFrame, variants: pd.DataFrame) -> dict:
    facts = pd.concat([sales, returns], ignore_index=True)
    with conn.cursor() as cur:
        # dim_date: todos los dias entre el primero y el ultimo (en UTC)
        if len(facts):
            days = pd.date_range(facts["invoice_ts_utc"].min().date(), facts["invoice_ts_utc"].max().date(), freq="D")
            date_frame = pd.DataFrame({
                "date_key": days.strftime("%Y%m%d").astype(int),
                "full_date": days.date,
                "year": days.year,
                "quarter": days.quarter,
                "month": days.month,
                "year_month": days.strftime("%Y-%m"),
                "day_of_month": days.day,
                "day_of_week": days.dayofweek + 1,
                "is_weekend": days.dayofweek >= 5,
            })
            cur.execute("CREATE TEMP TABLE stg_date (LIKE dw.dim_date) ON COMMIT DROP")
            _copy_frame(cur, date_frame, "stg_date", list(date_frame.columns))
            cur.execute("INSERT INTO dw.dim_date SELECT * FROM stg_date ON CONFLICT (date_key) DO NOTHING")

        # dim_country
        countries = pd.DataFrame({"country_name": sorted(facts["country"].dropna().unique())})
        cur.execute("CREATE TEMP TABLE stg_country (country_name TEXT) ON COMMIT DROP")
        _copy_frame(cur, countries, "stg_country", ["country_name"])
        cur.execute(
            "INSERT INTO dw.dim_country (country_name) SELECT country_name FROM stg_country "
            "ORDER BY country_name ON CONFLICT (country_name) DO NOTHING"
        )

        # dim_customer (la clave 0 'ANONYMOUS' ya existe desde el DDL)
        customers = pd.DataFrame({"customer_id": sorted(facts["customer_id"].dropna().unique())})
        cur.execute("CREATE TEMP TABLE stg_customer (customer_id TEXT) ON COMMIT DROP")
        _copy_frame(cur, customers, "stg_customer", ["customer_id"])
        cur.execute(
            "INSERT INTO dw.dim_customer (customer_id, is_identified) "
            "SELECT customer_id, TRUE FROM stg_customer ORDER BY customer_id "
            "ON CONFLICT (customer_id) DO NOTHING"
        )

        # dim_product (upsert: solo se toca la fila si algun atributo cambio)
        prod_cols = [
            "stock_code", "canonical_description", "category", "category_source", "supplier_country",
            "is_active", "raw_variant_count", "normalized_variant_count", "first_seen_utc", "last_seen_utc",
        ]
        product_frame = products.copy()
        product_frame["is_active"] = product_frame["is_active"].map(
            lambda v: None if pd.isna(v) else str(bool(v)).lower()
        )
        cur.execute(
            """CREATE TEMP TABLE stg_product (
                   stock_code TEXT, canonical_description TEXT, category TEXT, category_source TEXT,
                   supplier_country TEXT, is_active BOOLEAN, raw_variant_count INT,
                   normalized_variant_count INT, first_seen_utc TIMESTAMPTZ, last_seen_utc TIMESTAMPTZ
               ) ON COMMIT DROP"""
        )
        _copy_frame(cur, product_frame, "stg_product", prod_cols)
        cur.execute(
            """INSERT INTO dw.dim_product (stock_code, canonical_description, category, category_source,
                   supplier_country, is_active, raw_variant_count, normalized_variant_count,
                   first_seen_utc, last_seen_utc)
               SELECT * FROM stg_product ORDER BY stock_code
               ON CONFLICT (stock_code) DO UPDATE SET
                   canonical_description = EXCLUDED.canonical_description,
                   category = EXCLUDED.category,
                   category_source = EXCLUDED.category_source,
                   supplier_country = EXCLUDED.supplier_country,
                   is_active = EXCLUDED.is_active,
                   raw_variant_count = EXCLUDED.raw_variant_count,
                   normalized_variant_count = EXCLUDED.normalized_variant_count,
                   first_seen_utc = EXCLUDED.first_seen_utc,
                   last_seen_utc = EXCLUDED.last_seen_utc,
                   updated_at = now()
               WHERE (dw.dim_product.canonical_description, dw.dim_product.category, dw.dim_product.category_source,
                      dw.dim_product.supplier_country, dw.dim_product.is_active, dw.dim_product.raw_variant_count,
                      dw.dim_product.normalized_variant_count, dw.dim_product.first_seen_utc,
                      dw.dim_product.last_seen_utc)
                     IS DISTINCT FROM
                     (EXCLUDED.canonical_description, EXCLUDED.category, EXCLUDED.category_source,
                      EXCLUDED.supplier_country, EXCLUDED.is_active, EXCLUDED.raw_variant_count,
                      EXCLUDED.normalized_variant_count, EXCLUDED.first_seen_utc, EXCLUDED.last_seen_utc)"""
        )

        # product_description_variant: se reemplazan las variantes de los productos de esta carga
        cur.execute("CREATE TEMP TABLE stg_variant (stock_code TEXT, raw_description TEXT, occurrences INT) ON COMMIT DROP")
        _copy_frame(cur, variants, "stg_variant", ["stock_code", "raw_description", "occurrences"])
        cur.execute(
            "DELETE FROM dw.product_description_variant WHERE product_key IN "
            "(SELECT p.product_key FROM dw.dim_product p JOIN (SELECT DISTINCT stock_code FROM stg_product) s USING (stock_code))"
        )
        cur.execute(
            "INSERT INTO dw.product_description_variant (product_key, raw_description, occurrences) "
            "SELECT p.product_key, v.raw_description, v.occurrences FROM stg_variant v "
            "JOIN dw.dim_product p USING (stock_code)"
        )
        cur.execute("SELECT count(*) FROM dw.dim_product")
        return {"dim_product_rows": cur.fetchone()[0]}


# ---------------------------------------------------------------------------
# Hechos
# ---------------------------------------------------------------------------
_FACT_JOINS = """
    FROM {stage} s
    JOIN dw.dim_product  p ON p.stock_code = s.stock_code
    JOIN dw.dim_customer c ON c.customer_id = COALESCE(s.customer_id, 'ANONYMOUS')
    JOIN dw.dim_country  k ON k.country_name = s.country
    JOIN dw.dim_date     d ON d.full_date = (s.invoice_ts_utc AT TIME ZONE 'UTC')::date
"""


def load_facts(conn, sales: pd.DataFrame, returns: pd.DataFrame) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """CREATE TEMP TABLE stg_sales (
                   line_id UUID, invoice_no TEXT, invoice_ts_utc TIMESTAMPTZ, stock_code TEXT, customer_id TEXT,
                   country TEXT, quantity INT, unit_price NUMERIC(14,4), gross_revenue NUMERIC(16,4), source_system TEXT
               ) ON COMMIT DROP"""
        )
        _copy_frame(cur, sales, "stg_sales", [
            "line_id", "invoice_no", "invoice_ts_utc", "stock_code", "customer_id", "country",
            "quantity", "unit_price", "gross_revenue", "source_system",
        ])
        cur.execute(
            "INSERT INTO dw.fact_sales (line_id, invoice_no, invoice_ts_utc, date_key, product_key, customer_key, "
            "country_key, quantity, unit_price, gross_revenue, source_system) "
            "SELECT s.line_id, s.invoice_no, s.invoice_ts_utc, d.date_key, p.product_key, c.customer_key, "
            "k.country_key, s.quantity, s.unit_price, s.gross_revenue, s.source_system "
            + _FACT_JOINS.format(stage="stg_sales")
            + """ ON CONFLICT (line_id) DO UPDATE SET
                    invoice_no = EXCLUDED.invoice_no, invoice_ts_utc = EXCLUDED.invoice_ts_utc,
                    date_key = EXCLUDED.date_key, product_key = EXCLUDED.product_key,
                    customer_key = EXCLUDED.customer_key, country_key = EXCLUDED.country_key,
                    quantity = EXCLUDED.quantity, unit_price = EXCLUDED.unit_price,
                    gross_revenue = EXCLUDED.gross_revenue, source_system = EXCLUDED.source_system"""
        )
        sales_written = cur.rowcount

        cur.execute(
            """CREATE TEMP TABLE stg_returns (
                   line_id UUID, invoice_no TEXT, invoice_ts_utc TIMESTAMPTZ, stock_code TEXT, customer_id TEXT,
                   country TEXT, quantity_returned INT, unit_price NUMERIC(14,4), return_amount NUMERIC(16,4),
                   is_cancellation BOOLEAN, source_system TEXT
               ) ON COMMIT DROP"""
        )
        _copy_frame(cur, returns, "stg_returns", [
            "line_id", "invoice_no", "invoice_ts_utc", "stock_code", "customer_id", "country",
            "quantity_returned", "unit_price", "return_amount", "is_cancellation", "source_system",
        ])
        cur.execute(
            "INSERT INTO dw.fact_returns (line_id, invoice_no, invoice_ts_utc, date_key, product_key, customer_key, "
            "country_key, quantity_returned, unit_price, return_amount, is_cancellation_invoice, source_system) "
            "SELECT s.line_id, s.invoice_no, s.invoice_ts_utc, d.date_key, p.product_key, c.customer_key, "
            "k.country_key, s.quantity_returned, s.unit_price, s.return_amount, s.is_cancellation, s.source_system "
            + _FACT_JOINS.format(stage="stg_returns")
            + """ ON CONFLICT (line_id) DO UPDATE SET
                    invoice_no = EXCLUDED.invoice_no, invoice_ts_utc = EXCLUDED.invoice_ts_utc,
                    date_key = EXCLUDED.date_key, product_key = EXCLUDED.product_key,
                    customer_key = EXCLUDED.customer_key, country_key = EXCLUDED.country_key,
                    quantity_returned = EXCLUDED.quantity_returned, unit_price = EXCLUDED.unit_price,
                    return_amount = EXCLUDED.return_amount,
                    is_cancellation_invoice = EXCLUDED.is_cancellation_invoice,
                    source_system = EXCLUDED.source_system"""
        )
        returns_written = cur.rowcount

    if sales_written != len(sales) or returns_written != len(returns):
        raise RuntimeError(
            f"Se esperaban {len(sales)} ventas y {len(returns)} devoluciones; se escribieron "
            f"{sales_written} y {returns_written} (alguna fila no encontro su dimension)"
        )
    return {"sales_written": sales_written, "returns_written": returns_written}


# ---------------------------------------------------------------------------
# Rechazos, agregados y resumen
# ---------------------------------------------------------------------------
def load_rejections(conn, run_date: str, rejected: pd.DataFrame) -> dict:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM dw.rejected_records WHERE run_date = %s", (run_date,))
        if len(rejected):
            frame = rejected.copy()
            frame.insert(0, "run_date", run_date)
            _copy_frame(cur, frame, "dw.rejected_records", [
                "run_date", "source_system", "source_row", "reject_reason", "invoice_no_raw", "stock_code_raw", "payload",
            ])
    return {"rejected_written": len(rejected)}


def build_net_revenue_aggregate(conn) -> dict:
    """Revenue neto = bruto - devoluciones del MISMO codigo de producto en el MISMO dia (UTC)."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE dw.agg_daily_product_net")
        cur.execute(
            """INSERT INTO dw.agg_daily_product_net
                   (date_key, product_key, units_sold, units_returned, gross_revenue, returns_amount, net_revenue)
               WITH s AS (
                   SELECT date_key, product_key, SUM(quantity) AS units, SUM(gross_revenue) AS amount
                   FROM dw.fact_sales GROUP BY date_key, product_key
               ), r AS (
                   SELECT date_key, product_key, SUM(quantity_returned) AS units, SUM(return_amount) AS amount
                   FROM dw.fact_returns GROUP BY date_key, product_key
               )
               SELECT COALESCE(s.date_key, r.date_key), COALESCE(s.product_key, r.product_key),
                      COALESCE(s.units, 0), COALESCE(r.units, 0),
                      COALESCE(s.amount, 0), COALESCE(r.amount, 0),
                      COALESCE(s.amount, 0) - COALESCE(r.amount, 0)
               FROM s FULL OUTER JOIN r ON s.date_key = r.date_key AND s.product_key = r.product_key"""
        )
        return {"agg_rows": cur.rowcount}


def record_run_summary(conn, run_date: str, metrics: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO dw.etl_run_summary (run_date, metrics) VALUES (%s, %s) "
            "ON CONFLICT (run_date) DO UPDATE SET metrics = EXCLUDED.metrics, finished_at = now()",
            (run_date, json.dumps(metrics, default=str)),
        )


def verify_load(conn, expected_sales: int, expected_returns: int) -> dict:
    """Comprobaciones posteriores a la carga. Lanza error si algo no cuadra."""
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM dw.fact_sales")
        sales = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM dw.fact_returns")
        returns = cur.fetchone()[0]
        cur.execute(
            "SELECT (SELECT COALESCE(SUM(gross_revenue),0) FROM dw.fact_sales) - "
            "(SELECT COALESCE(SUM(return_amount),0) FROM dw.fact_returns) - "
            "(SELECT COALESCE(SUM(net_revenue),0) FROM dw.agg_daily_product_net)"
        )
        difference = float(cur.fetchone()[0])
    problems = []
    if sales < expected_sales:
        problems.append(f"fact_sales tiene {sales} filas y se cargaron {expected_sales}")
    if returns < expected_returns:
        problems.append(f"fact_returns tiene {returns} filas y se cargaron {expected_returns}")
    if abs(difference) > 0.01:
        problems.append(f"el neto agregado no cuadra con los hechos (diferencia {difference})")
    if problems:
        raise RuntimeError("; ".join(problems))
    return {"fact_sales_total": sales, "fact_returns_total": returns, "net_difference": difference}
