"""DAG diario del pipeline ETL de DataMart S.A.S.

validar -> extraer ventas (2 en paralelo) -> catalogo -> transformar -> calidad ->
dimensiones -> [hechos | rechazos] -> agregado neto -> verificacion
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from datamart import tasks
from datamart.sources import SOURCE_DAILY, SOURCE_HISTORY

default_args = {
    "owner": "data-engineering",
    "retries": 3,                              # reintentos automaticos ante fallos
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="datamart_sales_etl",
    description="Ventas (diario + historico) y catalogo de productos -> repositorio analitico PostgreSQL",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,                         # evita dos cargas simultaneas sobre las mismas tablas
    default_args=default_args,
    tags=["datamart", "etl"],
) as dag:

    validate_configuration = PythonOperator(
        task_id="validate_configuration",
        python_callable=tasks.validate_configuration,
    )
    extract_sales_daily = PythonOperator(
        task_id="extract_sales_daily",
        python_callable=tasks.extract_source,
        op_kwargs={"source_system": SOURCE_DAILY, "ds": "{{ ds }}"},
    )
    extract_sales_history = PythonOperator(
        task_id="extract_sales_history",
        python_callable=tasks.extract_source,
        op_kwargs={"source_system": SOURCE_HISTORY, "ds": "{{ ds }}"},
    )
    extract_products_catalog = PythonOperator(
        task_id="extract_products_catalog",
        python_callable=tasks.extract_products_catalog,
        op_kwargs={"ds": "{{ ds }}"},
    )
    transform_and_unify = PythonOperator(
        task_id="transform_and_unify",
        python_callable=tasks.transform_and_unify,
        op_kwargs={"ds": "{{ ds }}"},
    )
    run_quality_checks = PythonOperator(
        task_id="run_quality_checks",
        python_callable=tasks.run_quality_checks,
        op_kwargs={"ds": "{{ ds }}"},
    )
    load_dimensions = PythonOperator(
        task_id="load_dimensions",
        python_callable=tasks.load_dimensions,
        op_kwargs={"ds": "{{ ds }}"},
    )
    load_facts = PythonOperator(
        task_id="load_sales_and_returns",
        python_callable=tasks.load_facts,
        op_kwargs={"ds": "{{ ds }}"},
    )
    load_rejected_records = PythonOperator(
        task_id="load_rejected_records",
        python_callable=tasks.load_rejected_records,
        op_kwargs={"ds": "{{ ds }}"},
    )
    build_net_revenue = PythonOperator(
        task_id="build_net_revenue_aggregate",
        python_callable=tasks.build_net_revenue,
        op_kwargs={"ds": "{{ ds }}"},
    )
    verify_warehouse_load = PythonOperator(
        task_id="verify_warehouse_load",
        python_callable=tasks.verify_warehouse_load,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # El catalogo se consulta despues de extraer las ventas: asi la API ya ve los CSV (incluso si se acaban de descargar)
    validate_configuration >> [extract_sales_daily, extract_sales_history]
    [extract_sales_daily, extract_sales_history] >> extract_products_catalog >> transform_and_unify
    transform_and_unify >> run_quality_checks >> load_dimensions
    load_dimensions >> [load_facts, load_rejected_records] >> build_net_revenue >> verify_warehouse_load
