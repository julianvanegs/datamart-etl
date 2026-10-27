"""Consultas de solo lectura sobre el repositorio analitico (para dashboards en cualquier dispositivo)."""
from __future__ import annotations

import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras


@contextmanager
def _cursor():
    conn = psycopg2.connect(
        host=os.environ["DW_DB_HOST"],
        dbname=os.environ["DW_DB_NAME"],
        user=os.environ["DW_DB_USER"],
        password=os.environ["DW_DB_PASSWORD"],
        connect_timeout=5,
    )
    try:
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur
    finally:
        conn.close()


def _rows(sql: str, params: tuple = ()) -> list[dict]:
    with _cursor() as cur:
        cur.execute(sql, params)
        return [{k: (float(v) if hasattr(v, "as_tuple") else v) for k, v in row.items()} for row in cur.fetchall()]


def monthly_net_sales() -> list[dict]:
    return _rows("""
        SELECT d.year_month AS month,
               SUM(a.gross_revenue) AS gross_revenue,
               SUM(a.returns_amount) AS returns_amount,
               SUM(a.net_revenue) AS net_revenue
        FROM dw.agg_daily_product_net a
        JOIN dw.dim_date d USING (date_key)
        GROUP BY d.year_month ORDER BY d.year_month""")


def top_products(limit: int = 10) -> list[dict]:
    return _rows("""
        SELECT p.stock_code, p.canonical_description AS name, p.category,
               SUM(a.net_revenue) AS net_revenue
        FROM dw.agg_daily_product_net a
        JOIN dw.dim_product p USING (product_key)
        GROUP BY p.stock_code, p.canonical_description, p.category
        ORDER BY net_revenue DESC LIMIT %s""", (limit,))


def countries() -> list[dict]:
    return _rows("""
        SELECT c.country_name AS country,
               COUNT(DISTINCT s.invoice_no) AS invoices,
               SUM(s.gross_revenue) AS gross_revenue,
               ROUND(SUM(s.gross_revenue) / COUNT(DISTINCT s.invoice_no), 2) AS avg_ticket
        FROM dw.fact_sales s
        JOIN dw.dim_country c USING (country_key)
        GROUP BY c.country_name ORDER BY gross_revenue DESC""")
