"""API interna de productos + indicadores de solo lectura.

Pensada para integrarse facilmente desde cualquier dispositivo:
  * REST + JSON estandar y documentacion interactiva en /docs
  * CORS habilitado (apps web, moviles o herramientas BI pueden consumirla)
  * Escucha en 0.0.0.0 (accesible desde otros equipos de la red)
  * Panel web responsive en "/" que se ve bien en celular, tablet o PC
"""
from __future__ import annotations

import os
import secrets
import threading
from pathlib import Path

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from . import analytics
from .catalog import build_catalog, sources_signature

API_TOKEN = os.environ["PRODUCTS_API_TOKEN"]
STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(
    title="DataMart - API de productos e indicadores",
    version="1.0.0",
    description="Catalogo de productos (sistema legado simulado) e indicadores del repositorio analitico. "
                "Autenticacion: cabecera X-API-Key.",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["X-API-Key"],
)

_CATALOG: list[dict] = []
_BY_CODE: dict[str, dict] = {}
_SIGNATURE: tuple | None = None
_LOCK = threading.Lock()


def current_catalog() -> list[dict]:
    """Catalogo en memoria; se reconstruye si los CSV cambiaron (p. ej. el pipeline los acaba de descargar)."""
    global _CATALOG, _BY_CODE, _SIGNATURE
    signature = sources_signature()
    if signature != _SIGNATURE:
        with _LOCK:
            if signature != _SIGNATURE:
                _CATALOG = build_catalog()
                _BY_CODE = {p["code"]: p for p in _CATALOG}
                _SIGNATURE = signature
    return _CATALOG


@app.on_event("startup")
def load_catalog() -> None:
    current_catalog()


def require_token(x_api_key: str = Header(default="")) -> None:
    if not secrets.compare_digest(x_api_key, API_TOKEN):
        raise HTTPException(status_code=401, detail="X-API-Key invalida o ausente")


@app.get("/health", tags=["sistema"])
def health() -> dict:
    return {"status": "ok", "products_loaded": len(_CATALOG)}


@app.get("/", include_in_schema=False)
def dashboard() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/products", tags=["productos"], dependencies=[Depends(require_token)])
def list_products(
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
    category: str | None = None,
    active: bool | None = None,
) -> dict:
    items = current_catalog()
    if category:
        items = [p for p in items if p["category"].lower() == category.lower()]
    if active is not None:
        items = [p for p in items if p["is_active"] == active]
    start = (page - 1) * page_size
    return {
        "page": page,
        "page_size": page_size,
        "total": len(items),
        "total_pages": max(1, -(-len(items) // page_size)),
        "items": items[start:start + page_size],
    }


@app.get("/products/{code}", tags=["productos"], dependencies=[Depends(require_token)])
def get_product(code: str) -> dict:
    current_catalog()
    normalized = "".join(code.upper().split())
    product = _BY_CODE.get(normalized)
    if product is None:
        raise HTTPException(status_code=404, detail=f"Producto {normalized} no encontrado")
    return product


def _safe(fn, *args):
    try:
        return fn(*args)
    except Exception as exc:  # el DW puede estar vacio o no disponible aun
        raise HTTPException(status_code=503, detail=f"Repositorio analitico no disponible: {exc}") from exc


@app.get("/analytics/monthly-net-sales", tags=["indicadores"], dependencies=[Depends(require_token)])
def monthly_net_sales() -> list[dict]:
    return _safe(analytics.monthly_net_sales)


@app.get("/analytics/top-products", tags=["indicadores"], dependencies=[Depends(require_token)])
def top_products(limit: int = Query(10, ge=1, le=100)) -> list[dict]:
    return _safe(analytics.top_products, limit)


@app.get("/analytics/countries", tags=["indicadores"], dependencies=[Depends(require_token)])
def countries() -> list[dict]:
    return _safe(analytics.countries)
