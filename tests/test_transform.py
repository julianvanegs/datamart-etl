"""Pruebas de las reglas de negocio (no requieren Airflow ni base de datos)."""
import pandas as pd
import pytest

from datamart import quality, sources, transform

PATTERN = r"^(\d{5}[A-Z]{0,3}|DCGS\d{4}[A-Z]?)$"


def raw_frame(rows):
    cols = ["invoice_no", "stock_code", "description", "quantity", "invoice_date", "unit_price", "customer_id", "country"]
    frame = pd.DataFrame(rows, columns=cols).astype(object).where(lambda d: d.notna(), None)
    frame.insert(0, "source_row", range(2, len(frame) + 2))
    return frame


def daily(rows):
    return transform.clean_source(raw_frame(rows), sources.SOURCE_DAILY, "Europe/London", PATTERN)


def row(**kw):
    base = dict(invoice_no="536365", stock_code="85123A", description="WHITE HANGING HEART T-LIGHT HOLDER",
                quantity="6", invoice_date="12/1/2010 8:26", unit_price="2.55", customer_id="17850.0", country="United Kingdom")
    base.update(kw)
    return list(base.values())


def test_valid_row_is_kept_and_normalized():
    clean, rejected = daily([row(stock_code=" 85123a ", customer_id="17850.0")])
    assert len(rejected) == 0
    assert clean.loc[0, "stock_code"] == "85123A"
    assert clean.loc[0, "customer_id"] == "17850"


def test_dates_are_converted_to_utc():
    clean, _ = daily([row(invoice_date="7/1/2011 9:00")])      # verano: Londres = UTC+1
    assert str(clean.loc[0, "invoice_ts_utc"]) == "2011-07-01 08:00:00+00:00"
    clean, _ = daily([row(invoice_date="12/1/2010 8:26")])     # invierno: Londres = UTC
    assert str(clean.loc[0, "invoice_ts_utc"]) == "2010-12-01 08:26:00+00:00"


@pytest.mark.parametrize("overrides,reason", [
    (dict(unit_price="0"), transform.R_PRICE_SALE),
    (dict(unit_price="-3"), transform.R_PRICE_SALE),
    (dict(quantity="-2", unit_price="0"), transform.R_PRICE_RETURN),
    (dict(stock_code="POST"), transform.R_NON_CATALOG),
    (dict(stock_code="gift_0001_20"), transform.R_NON_CATALOG),
    (dict(stock_code=" "), transform.R_MISSING_STOCK),
    (dict(invoice_date="not a date"), transform.R_BAD_DATE),
    (dict(quantity="abc"), transform.R_BAD_QUANTITY),
])
def test_rejections_have_a_reason(overrides, reason):
    clean, rejected = daily([row(**overrides)])
    assert len(clean) == 0
    assert rejected.loc[0, "reject_reason"] == reason
    assert rejected.loc[0, "source_system"] == sources.SOURCE_DAILY


def test_missing_customer_is_kept_as_anonymous():
    clean, rejected = daily([row(customer_id=None)])
    assert len(rejected) == 0 and pd.isna(clean.loc[0, "customer_id"])


def test_exact_duplicates_within_source_are_rejected_once():
    clean, rejected = daily([row(), row()])
    assert len(clean) == 1
    assert list(rejected["reject_reason"]) == [transform.R_DUP_SOURCE]


def test_negative_quantity_goes_to_returns():
    clean, _ = daily([row(), row(invoice_no="C536379", quantity="-4", invoice_date="12/1/2010 9:41")])
    sales, returns = transform.split_sales_and_returns(clean)
    assert len(sales) == 1 and len(returns) == 1
    assert returns.loc[0, "quantity_returned"] == 4
    assert returns.loc[0, "return_amount"] == pytest.approx(4 * 2.55)
    assert sales.loc[0, "gross_revenue"] == pytest.approx(6 * 2.55)


def test_cross_source_overlap_keeps_daily_and_rejects_history():
    d, _ = daily([row()])
    h_raw = raw_frame([row(invoice_date="2010-12-01 08:26:00", stock_code="85123a")])
    h, _ = transform.clean_source(h_raw, sources.SOURCE_HISTORY, "Europe/London", PATTERN)
    unified, rejected = transform.unify_sources({sources.SOURCE_DAILY: d, sources.SOURCE_HISTORY: h})
    assert len(unified) == 1 and unified.loc[0, "source_system"] == sources.SOURCE_DAILY
    assert list(rejected["reject_reason"]) == [transform.R_DUP_CROSS]


def test_canonical_description_is_most_frequent_ignoring_case():
    rows = [row(invoice_no=str(i), description=d) for i, d in enumerate(
        ["CANDLE HOLDER WHITE", "Candle Holder White", "candle holder white", "CANDLE HOLDER WHITE  ", "CANDLE HOLDER IVORY"])]
    clean, _ = daily(rows)
    products, variants = transform.build_product_master(clean)
    assert products.loc[0, "canonical_description"] == "CANDLE HOLDER WHITE"
    assert products.loc[0, "normalized_variant_count"] == 2      # WHITE e IVORY son textos realmente distintos
    assert products.loc[0, "raw_variant_count"] == 4


def test_line_id_is_deterministic():
    a, _ = daily([row()])
    b, _ = daily([row()])
    assert a.loc[0, "line_id"] == b.loc[0, "line_id"]


def test_quality_checks_fail_when_rejection_ratio_is_too_high():
    clean, rejected = daily([row(), row(unit_price="0", invoice_no="2"), row(unit_price="0", invoice_no="3")])
    sales, returns = transform.split_sales_and_returns(clean)
    products, _ = transform.build_product_master(clean)
    products = transform.enrich_with_catalog(products, None)
    quality.run_quality_checks(sales, returns, products, rejected, 3, 0.80)
    with pytest.raises(ValueError):
        quality.run_quality_checks(sales, returns, products, rejected, 3, 0.10)


def test_category_fallback_without_api():
    clean, _ = daily([row(stock_code="22086", description="PAPER CHAIN KIT 50'S CHRISTMAS")])
    products, _ = transform.build_product_master(clean)
    enriched = transform.enrich_with_catalog(products, None)
    assert enriched.loc[0, "category"] == "Papelería" and enriched.loc[0, "category_source"] == "keyword_rules"


def test_category_from_api_has_priority():
    clean, _ = daily([row(stock_code="22086", description="PAPER CHAIN KIT")])
    products, _ = transform.build_product_master(clean)
    catalog = pd.DataFrame([{"code": "22086", "name": "X", "category": "Hogar", "supplier_country": "Perú", "is_active": True}])
    enriched = transform.enrich_with_catalog(products, catalog)
    assert enriched.loc[0, "category"] == "Hogar" and enriched.loc[0, "category_source"] == "products_api"
