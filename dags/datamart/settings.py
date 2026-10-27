"""Parametros operativos del pipeline.

Los valores vienen de Airflow Variables (inicializadas automaticamente al levantar
el entorno por scripts/bootstrap_airflow.sh). Nada sensible vive en el codigo.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DW_CONNECTION_ID = "datamart_dw"
API_CONNECTION_ID = "products_api"

DATA_DIR = Path(os.environ.get("DATAMART_DATA_DIR", "/opt/airflow/data"))
SEEDS_DIR = Path(os.environ.get("DATAMART_SEEDS_DIR", "/opt/airflow/seeds"))

REQUIRED_VARIABLES = (
    "datamart_source_timezone",
    "datamart_max_rejection_ratio",
    "datamart_use_products_api",
    "datamart_api_page_size",
    "datamart_stock_code_pattern",
    "datamart_sales_daily_file",
    "datamart_sales_history_file",
    "datamart_kaggle_dataset_sales_daily",
    "datamart_kaggle_dataset_sales_history",
)


@dataclass(frozen=True)
class Settings:
    source_timezone: str
    max_rejection_ratio: float
    use_products_api: bool
    api_page_size: int
    stock_code_pattern: str
    sales_daily_file: str
    sales_history_file: str
    kaggle_dataset_sales_daily: str
    kaggle_dataset_sales_history: str

    @classmethod
    def load(cls) -> "Settings":
        from airflow.models import Variable  # import tardio: el modulo es testeable sin Airflow

        missing = [v for v in REQUIRED_VARIABLES if Variable.get(v, default_var=None) in (None, "")]
        if missing:
            raise RuntimeError(f"Faltan Airflow Variables: {missing}")
        return cls(
            source_timezone=Variable.get("datamart_source_timezone"),
            max_rejection_ratio=float(Variable.get("datamart_max_rejection_ratio")),
            use_products_api=Variable.get("datamart_use_products_api").strip().lower() == "true",
            api_page_size=int(Variable.get("datamart_api_page_size")),
            stock_code_pattern=Variable.get("datamart_stock_code_pattern"),
            sales_daily_file=Variable.get("datamart_sales_daily_file"),
            sales_history_file=Variable.get("datamart_sales_history_file"),
            kaggle_dataset_sales_daily=Variable.get("datamart_kaggle_dataset_sales_daily"),
            kaggle_dataset_sales_history=Variable.get("datamart_kaggle_dataset_sales_history"),
        )


def staging_dir(run_date: str) -> Path:
    path = DATA_DIR / "staging" / run_date
    path.mkdir(parents=True, exist_ok=True)
    return path
