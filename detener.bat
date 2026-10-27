@echo off
rem Apaga los servicios (conserva los datos). Para borrar tambien las bases: docker compose down -v
cd /d "%~dp0"
docker compose down
pause
