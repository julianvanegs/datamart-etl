#!/usr/bin/env bash
# Apaga los servicios (conserva los datos). Para borrar tambien las bases:  docker compose down -v
cd "$(dirname "$0")"
docker compose down
