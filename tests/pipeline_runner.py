"""Ejecuta las etapas del pipeline sin Airflow (para pruebas). Mismo codigo que usan las tareas del DAG."""
import pandas as pd

from datamart import loaders, quality, sources, transform

PATTERN = r"^(\d{5}[A-Z]{0,3}|DCGS\d{4}[A-Z]?)$"
TZ = "Europe/London"


def run(conn, run_date, daily_path, history_path, catalog=None):
    raws = {
        sources.SOURCE_DAILY: sources.read_source_csv(daily_path),
        sources.SOURCE_HISTORY: sources.read_source_csv(history_path),
    }
    cleaned, rejections = {}, []
    for name, raw in raws.items():
        cleaned[name], rejected = transform.clean_source(raw, name, TZ, PATTERN)
        rejections.append(rejected)
    unified, cross = transform.unify_sources(cleaned)
    rejected_all = pd.concat(rejections + [cross], ignore_index=True)
    sales, returns = transform.split_sales_and_returns(unified)
    products, variants = transform.build_product_master(unified)
    products = transform.enrich_with_catalog(products, catalog)
    rows_in = sum(len(r) for r in raws.values())
    quality.run_quality_checks(sales, returns, products, rejected_all, rows_in, 0.30)

    loaders.load_dimensions(conn, sales, returns, products, variants); conn.commit()
    loaders.load_facts(conn, sales, returns); conn.commit()
    loaders.load_rejections(conn, run_date, rejected_all); conn.commit()
    loaders.build_net_revenue_aggregate(conn); conn.commit()
    check = loaders.verify_load(conn, len(sales), len(returns)); conn.commit()
    return {"sales": len(sales), "returns": len(returns), "rejected": len(rejected_all), "products": len(products), "verify": check,
            "reasons": rejected_all.groupby(["source_system", "reject_reason"]).size().to_dict()}
