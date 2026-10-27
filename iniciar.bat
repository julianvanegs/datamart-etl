@echo off
rem Inicia todo el entorno en Windows. Haz doble clic en este archivo.
cd /d "%~dp0"

where docker >nul 2>nul
if errorlevel 1 (
  echo Docker no esta instalado. Instalalo desde https://www.docker.com/products/docker-desktop y vuelve a abrir este archivo.
  pause
  exit /b 1
)
docker info >nul 2>nul
if errorlevel 1 (
  echo Docker esta instalado pero no esta abierto. Abre Docker Desktop, espera a que diga "running" y vuelve a abrir este archivo.
  pause
  exit /b 1
)

if not exist .env (
  copy .env.example .env >nul
  echo Se creo el archivo .env con valores por defecto ^(puedes editarlo^).
)

echo Levantando servicios ^(la primera vez tarda unos minutos^)...
docker compose up --build -d
if errorlevel 1 (
  echo Algo fallo al levantar los servicios. Revisa el mensaje de arriba.
  pause
  exit /b 1
)

echo.
echo Listo. En 1-2 minutos abre:  http://localhost:8080   ^(usuario y clave: los de AIRFLOW_ADMIN_* en el archivo .env^)
echo El DAG datamart_sales_etl se ejecuta solo. Para apagar todo abre detener.bat
pause
