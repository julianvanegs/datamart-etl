#!/usr/bin/env bash
# Se ejecuta UNA vez al levantar el entorno (servicio "airflow-init").
# 1) Migra la base de metadatos de Airflow
# 2) Crea el usuario administrador
# 3) Registra las Airflow Connections
# 4) Inicializa las Airflow Variables
# Es idempotente: se puede ejecutar varias veces sin duplicar nada.
set -euo pipefail

echo ">> Migrando base de metadatos de Airflow..."
airflow db migrate

echo ">> Creando usuario administrador (si no existe)..."
airflow users create \
  --username "${AIRFLOW_ADMIN_USER}" \
  --password "${AIRFLOW_ADMIN_PASSWORD}" \
  --firstname Admin --lastname DataMart \
  --role Admin \
  --email "${AIRFLOW_ADMIN_EMAIL}" || echo "   (el usuario ya existia)"

echo ">> Registrando Connections..."
airflow connections delete datamart_dw >/dev/null 2>&1 || true
airflow connections add datamart_dw \
  --conn-type postgres \
  --conn-host "${DW_DB_HOST}" \
  --conn-port 5432 \
  --conn-schema "${DW_DB_NAME}" \
  --conn-login "${DW_DB_USER}" \
  --conn-password "${DW_DB_PASSWORD}"

airflow connections delete products_api >/dev/null 2>&1 || true
airflow connections add products_api \
  --conn-type http \
  --conn-host "${PRODUCTS_API_HOST}" \
  --conn-port 8000 \
  --conn-schema http \
  --conn-password "${PRODUCTS_API_TOKEN}"

echo ">> Inicializando Variables..."
airflow variables set datamart_source_timezone   "${DATAMART_SOURCE_TIMEZONE}"
airflow variables set datamart_max_rejection_ratio "${DATAMART_MAX_REJECTION_RATIO}"
airflow variables set datamart_use_products_api  "${DATAMART_USE_PRODUCTS_API}"
airflow variables set datamart_api_page_size     "${DATAMART_API_PAGE_SIZE}"
# Patron de codigos de producto validos del catalogo (5 digitos + hasta 3 letras, o codigos DCGS)
airflow variables set datamart_stock_code_pattern '^(\d{5}[A-Z]{0,3}|DCGS\d{4}[A-Z]?)$'
airflow variables set datamart_sales_daily_file  "${DATAMART_SALES_DAILY_FILE}"
airflow variables set datamart_sales_history_file "${DATAMART_SALES_HISTORY_FILE}"
airflow variables set datamart_kaggle_dataset_sales_daily   "${KAGGLE_DATASET_SALES_DAILY}"
airflow variables set datamart_kaggle_dataset_sales_history "${KAGGLE_DATASET_SALES_HISTORY}"

echo ">> Listo: Connections y Variables configuradas."
