"""Cliente de la API interna de productos."""
from __future__ import annotations

import logging
import time

import pandas as pd
import requests

log = logging.getLogger(__name__)


def fetch_catalog(base_url: str, token: str, page_size: int, attempts: int = 5, wait_seconds: int = 10) -> pd.DataFrame:
    """Descarga todo el catalogo recorriendo la paginacion. Reintenta si la API aun esta arrancando."""
    headers = {"X-API-Key": token}
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            items: list[dict] = []
            page = 1
            while True:
                response = requests.get(
                    f"{base_url}/products", headers=headers, params={"page": page, "page_size": page_size}, timeout=180
                )
                response.raise_for_status()
                body = response.json()
                items.extend(body["items"])
                if page >= body["total_pages"]:
                    break
                page += 1
            log.info("Catalogo descargado: %s productos", len(items))
            return pd.DataFrame(items)
        except requests.RequestException as exc:
            last_error = exc
            log.warning("API de productos no disponible (intento %s/%s): %s", attempt, attempts, exc)
            time.sleep(wait_seconds)
    raise ConnectionError(f"No se pudo consultar la API de productos: {last_error}")
