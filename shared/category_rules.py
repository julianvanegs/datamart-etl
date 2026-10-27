"""Reglas de categorizacion de productos por palabras clave.

Este modulo lo usan DOS componentes, para que den siempre el mismo resultado:
  * la API de productos (catalogo legado simulado), y
  * el pipeline, como estrategia de respaldo cuando la API no esta disponible.

Las cinco categorias son las del catalogo de DataMart:
Electronica, Hogar, Ropa, Deportes y Papeleria.
"""
from __future__ import annotations

import re

CATEGORIES = ("Electrónica", "Hogar", "Ropa", "Deportes", "Papelería")
DEFAULT_CATEGORY = "Hogar"

# El orden importa: se evalua de arriba hacia abajo y gana la primera coincidencia.
_KEYWORDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Electrónica", (
        "LIGHTS", "LAMP", "LED", "RADIO", "CLOCK", "ALARM", "TORCH", "CALCULATOR",
        "BATTERY", "SPEAKER", "CHARGER", "CABLE", "ELECTRIC", "FLASHING", "BULB",
    )),
    ("Deportes", (
        "SKIPPING", "FOOTBALL", "FRISBEE", "CRICKET", "DARTS", "YOGA",
        "SPORT", "BOOMERANG", "KITE", "GAME", "BICYCLE", "FITNESS", "JUMP",
    )),
    ("Ropa", (
        "SCARF", "HAT", "SOCK", "APRON", "BIB", "GLOVE", "JUMPER",
        "SHIRT", "SHAWL", "BRACELET", "NECKLACE", "EARRING", "HAIR",
        "SLIPPER", "PONCHO",
    )),
    ("Papelería", (
        "PAPER", "CARD", "NOTEBOOK", "PEN", "PENCIL", "STICKER", "WRAP",
        "ENVELOPE", "TAG", "STATIONERY", "RIBBON", "STAMP", "RUBBER", "ERASER",
        "RULER", "CALENDAR", "DIARY", "JOURNAL", "BOOK", "POSTCARD", "CRAYON",
    )),
)


def _words(description: str) -> set[str]:
    """Palabras en mayusculas, separando guiones y signos (T-LIGHT -> T, LIGHT)."""
    return set(re.findall(r"[A-Z0-9]+", description.upper()))


def classify_description(description: str | None) -> str:
    """Devuelve la categoria de un producto a partir de su descripcion.

    Coincide por palabra completa (singular o plural) para evitar falsos positivos
    como "HAT" dentro de "WHAT".
    """
    if not description:
        return DEFAULT_CATEGORY
    words = _words(description)
    for category, keywords in _KEYWORDS:
        for keyword in keywords:
            if keyword in words or f"{keyword}S" in words or f"{keyword}ES" in words:
                return category
    return DEFAULT_CATEGORY


SUPPLIER_COUNTRIES = ("Colombia", "México", "Perú", "China", "Reino Unido")


def supplier_country_for(stock_code: str) -> str:
    """País de origen del proveedor, determinista por código (simulación del sistema legado)."""
    # Suma de bytes: estable entre ejecuciones (a diferencia de hash(), que se aleatoriza).
    return SUPPLIER_COUNTRIES[sum(stock_code.encode("utf-8")) % len(SUPPLIER_COUNTRIES)]
