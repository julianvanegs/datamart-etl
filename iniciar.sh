#!/usr/bin/env bash
# Inicia todo el entorno (Mac y Linux). Uso:  ./iniciar.sh
set -e
cd "$(dirname "$0")"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker no esta instalado. Instalalo desde https://www.docker.com/products/docker-desktop y vuelve a ejecutar este archivo."
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "Docker esta instalado pero no esta abierto. Abre Docker Desktop, espera a que diga 'running' y vuelve a ejecutar."
  exit 1
fi

if [ ! -f .env ]; then
  cp .env.example .env
  echo "Se creo el archivo .env con valores por defecto (puedes editarlo)."
fi

# En Linux, los archivos que cree Airflow en data/ deben quedar a nombre de tu usuario
if [ "$(uname -s)" = "Linux" ]; then
  grep -v '^AIRFLOW_UID=' .env > .env.tmp || true
  echo "AIRFLOW_UID=$(id -u)" >> .env.tmp
  mv .env.tmp .env
fi

echo "Levantando servicios (la primera vez tarda unos minutos)..."
docker compose up --build -d

PORT=$(grep '^AIRFLOW_WEB_PORT=' .env | cut -d= -f2); PORT=${PORT:-8080}
USER_NAME=$(grep '^AIRFLOW_ADMIN_USER=' .env | cut -d= -f2)
echo ""
echo "Listo. En 1-2 minutos abre:  http://localhost:${PORT}   (usuario: ${USER_NAME}, clave: la de AIRFLOW_ADMIN_PASSWORD en .env)"
echo "El DAG 'datamart_sales_etl' se ejecuta solo. Para apagar todo:  ./detener.sh"
