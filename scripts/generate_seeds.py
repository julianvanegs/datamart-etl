"""Genera los datos de prueba (seeds/) con los mismos problemas de calidad que los datasets reales.

Uso (opcional; los CSV ya vienen incluidos en el repositorio):
    python scripts/generate_seeds.py
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

SEEDS = Path(__file__).resolve().parent.parent / "seeds"
rng = random.Random(42)

PRODUCTS = [  # codigo, descripcion, precio base
    ("85123A", "WHITE HANGING HEART T-LIGHT HOLDER", 2.55), ("71053", "WHITE METAL LANTERN", 3.39),
    ("84406B", "CREAM CUPID HEARTS COAT HANGER", 2.75), ("84029G", "KNITTED UNION FLAG HOT WATER BOTTLE", 3.39),
    ("22752", "SET 7 BABUSHKA NESTING BOXES", 7.65), ("21730", "GLASS STAR FROSTED T-LIGHT HOLDER", 4.25),
    ("22633", "HAND WARMER UNION JACK", 1.85), ("22632", "HAND WARMER RED POLKA DOT", 1.85),
    ("84879", "ASSORTED COLOUR BIRD ORNAMENT", 1.69), ("22745", "POPPY'S PLAYHOUSE BEDROOM", 2.10),
    ("22748", "POPPY'S PLAYHOUSE KITCHEN", 2.10), ("22086", "PAPER CHAIN KIT 50'S CHRISTMAS", 2.55),
    ("22960", "JAM MAKING SET WITH JARS", 4.25), ("22139", "RETROSPOT TEA SET CERAMIC 11 PC", 4.95),
    ("21212", "PACK OF 72 RETROSPOT CAKE CASES", 0.55), ("22423", "REGENCY CAKESTAND 3 TIER", 12.75),
    ("47566", "PARTY BUNTING", 4.95), ("20725", "LUNCH BAG RED RETROSPOT", 1.65),
    ("22197", "SMALL POPCORN HOLDER", 0.85), ("22961", "JAM MAKING SET PRINTED", 1.45),
    ("22178", "VICTORIAN GLASS HANGING T-LIGHT", 1.25), ("85099B", "JUMBO BAG RED RETROSPOT", 2.08),
    ("22111", "SCOTTIE DOG HOT WATER BOTTLE", 4.95), ("21034", "REX CASH+CARRY JUMBO SHOPPER", 0.95),
    ("23084", "RABBIT NIGHT LIGHT", 2.08), ("22469", "HEART OF WICKER SMALL", 1.65),
    ("22300", "COFFEE MUG DOG + BALL DESIGN", 2.49), ("22086A", "CHRISTMAS GIFT WRAP PAPER SET", 1.95),
    ("21977", "PACK OF 60 PINK PAISLEY CAKE CASES", 0.55), ("22554", "PLASTERS IN TIN WOODLAND ANIMALS", 1.65),
    ("22727", "ALARM CLOCK BAKELIKE RED", 3.75), ("22728", "ALARM CLOCK BAKELIKE PINK", 3.75),
    ("22915", "ASSORTED BOTTLE TOP MAGNETS", 0.42), ("21175", "GIN + TONIC DIET METAL SIGN", 2.10),
    ("22386", "JUMBO BAG PINK POLKADOT", 2.08), ("22189", "CREAM HEART CARD HOLDER", 3.95),
    ("23166", "MEDIUM CERAMIC TOP STORAGE JAR", 1.04), ("22505", "MEMO BOARD COTTAGE DESIGN", 4.95),
    ("21931", "JUMBO STORAGE BAG SUKI", 2.08), ("22666", "RECIPE BOX PANTRY YELLOW DESIGN", 2.95),
    ("22910", "PAPER CHAIN KIT VINTAGE CHRISTMAS", 2.95), ("20750", "RED RETROSPOT MINI CASES", 7.95),
    ("21080", "SET/20 RED RETROSPOT PAPER NAPKINS", 0.85), ("22960B", "SKIPPING ROPE RETRO", 2.95),
    ("22992", "REVOLVER WOODEN RULER", 1.95), ("21790", "VINTAGE SNAP CARDS", 0.85),
    ("22423B", "WOOLLY HAT KNITTED STRIPE", 5.45), ("22386B", "BABY BIB COTTON ANIMALS", 2.25),
]
COUNTRIES = ["United Kingdom"] * 14 + ["France", "Germany", "EIRE", "Spain", "Netherlands", "Australia"]
CUSTOMERS = [12346 + 37 * i for i in range(60)]


def vary_description(text):
    """Variantes de escritura del mismo producto, como en los datos reales."""
    roll = rng.random()
    if roll < 0.70:
        return text
    if roll < 0.82:
        return text.title()
    if roll < 0.90:
        return text.lower()
    if roll < 0.95:
        return text + "  "
    return "  " + text


def vary_code(code):
    roll = rng.random()
    if roll < 0.93:
        return code
    if roll < 0.97:
        return code.lower()
    return f" {code[:2]} {code[2:]} "


def build_rows(start, days, n_invoices, first_invoice, with_overlap_from=None):
    rows = []
    invoice = first_invoice
    for _ in range(n_invoices):
        invoice += 1
        when = start + timedelta(days=rng.randrange(days), hours=rng.randrange(8, 18), minutes=rng.randrange(60))
        customer = rng.choice(CUSTOMERS) if rng.random() > 0.15 else None
        country = rng.choice(COUNTRIES)
        is_cancel = rng.random() < 0.06
        inv_no = f"C{invoice}" if is_cancel else str(invoice)
        for _ in range(rng.randrange(1, 6)):
            code, desc, price = rng.choice(PRODUCTS)
            qty = rng.choice([1, 2, 3, 4, 6, 8, 12, 24])
            rows.append({
                "invoice": inv_no, "code": vary_code(code), "desc": vary_description(desc),
                "qty": -qty if is_cancel else qty, "when": when, "price": round(price * rng.uniform(0.95, 1.1), 2),
                "customer": customer, "country": country,
            })
    return rows


def inject_problems(rows):
    """Casos que el pipeline debe rechazar o resolver."""
    when = rows[0]["when"]
    extras = [
        dict(invoice="9000001", code="POST", desc="POSTAGE", qty=1, when=when, price=18.0, customer=12346, country="France"),
        dict(invoice="9000002", code="D", desc="Discount", qty=-1, when=when, price=12.5, customer=12346, country="France"),
        dict(invoice="9000003", code="M", desc="Manual", qty=1, when=when, price=55.0, customer=None, country="United Kingdom"),
        dict(invoice="9000004", code="BANK CHARGES", desc="Bank Charges", qty=1, when=when, price=15.0, customer=None, country="United Kingdom"),
        dict(invoice="9000005", code="gift_0001_20", desc="Dotcomgiftshop Gift Voucher 20", qty=1, when=when, price=16.67, customer=None, country="United Kingdom"),
        dict(invoice="9000006", code="85123A", desc="WHITE HANGING HEART T-LIGHT HOLDER", qty=5, when=when, price=0.0, customer=12383, country="United Kingdom"),
        dict(invoice="9000007", code="22423", desc="REGENCY CAKESTAND 3 TIER", qty=-3, when=when, price=0.0, customer=None, country="United Kingdom"),
        dict(invoice="9000008", code="22752", desc="SET 7 BABUSHKA NESTING BOXES", qty=2, when=when, price=-7.65, customer=12383, country="United Kingdom"),
        dict(invoice="9000009", code="22633", desc=None, qty=3, when=when, price=1.85, customer=12420, country="United Kingdom"),
        dict(invoice="9000010", code="22633", desc="HAND WARMER UNION JACK", qty=2, when=None, price=1.85, customer=12420, country="United Kingdom"),
        dict(invoice="9000011", code="22633", desc="HAND WARMER UNION JACK", qty=2,
             when=datetime(2011, 3, 27, 1, 30), price=1.85, customer=12420, country="United Kingdom"),  # hora inexistente (DST)
        dict(invoice="9000012", code="22633", desc="HAND WARMER UNION JACK", qty=2,
             when=datetime(2010, 10, 31, 1, 30), price=1.85, customer=12420, country="United Kingdom"),  # hora ambigua (DST)
    ]
    rows.extend(extras)
    rows.extend(dict(r) for r in rng.sample(rows[:200], 12))  # duplicados exactos
    return rows


def write_daily(rows, path):
    with open(path, "w", newline="", encoding="latin-1") as fh:
        w = csv.writer(fh)
        w.writerow(["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"])
        for r in rows:
            d = r["when"]
            stamp = "" if d is None else f"{d.month}/{d.day}/{d.year} {d.hour}:{d.minute:02d}"
            cust = "" if r["customer"] is None else f"{r['customer']}.0"
            w.writerow([r["invoice"], r["code"], r["desc"] or "", r["qty"], stamp, r["price"], cust, r["country"]])


def write_history(rows, path):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["Invoice", "StockCode", "Description", "Quantity", "InvoiceDate", "Price", "Customer ID", "Country"])
        for r in rows:
            d = r["when"]
            stamp = "" if d is None else d.strftime("%Y-%m-%d %H:%M:%S")
            cust = "" if r["customer"] is None else f"{r['customer']}.0"
            country = {"EIRE": "Ireland"}.get(r["country"], r["country"])  # el historico usa otros nombres
            w.writerow([r["invoice"], r["code"], r["desc"] or "", r["qty"], stamp, r["price"], cust, country])


if __name__ == "__main__":
    SEEDS.mkdir(exist_ok=True)
    daily = inject_problems(build_rows(datetime(2011, 1, 1), 150, 260, 540000))
    history = build_rows(datetime(2010, 1, 1), 440, 260, 490000)
    # Solapamiento: el historico repite lineas del volcado diario (enero-marzo 2011), con la hora en formato ISO
    overlap = [dict(r) for r in daily if r["when"] and r["when"] < datetime(2011, 4, 1) and r["price"] > 0][:80]
    history.extend(overlap)
    write_daily(daily, SEEDS / "sales_daily_sample.csv")
    write_history(history, SEEDS / "sales_history_sample.csv")
    print(f"seeds generados: diario={len(daily)} filas, historico={len(history)} filas, solapadas={len(overlap)}")
