-- ============================================================
-- Hechos separados (ventas vs devoluciones), rechazos y control
-- ============================================================
SET search_path TO dw, public;

-- Ventas: solo cantidades > 0 y precio > 0
CREATE TABLE IF NOT EXISTS fact_sales (
    line_id         UUID PRIMARY KEY,                 -- hash determinista de la linea (idempotencia)
    invoice_no      TEXT        NOT NULL,
    invoice_ts_utc  TIMESTAMPTZ NOT NULL,
    date_key        INTEGER     NOT NULL REFERENCES dim_date (date_key),
    product_key     INTEGER     NOT NULL REFERENCES dim_product (product_key),
    customer_key    INTEGER     NOT NULL REFERENCES dim_customer (customer_key),
    country_key    INTEGER     NOT NULL REFERENCES dim_country (country_key),
    quantity        INTEGER     NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(14,4) NOT NULL CHECK (unit_price > 0),
    gross_revenue   NUMERIC(16,4) NOT NULL,           -- cantidad x precio unitario
    source_system   TEXT        NOT NULL,             -- 'sales_daily' | 'sales_history'
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Devoluciones y ajustes: cantidad <= 0 (se guarda la cantidad devuelta en positivo)
CREATE TABLE IF NOT EXISTS fact_returns (
    line_id                 UUID PRIMARY KEY,
    invoice_no              TEXT        NOT NULL,
    invoice_ts_utc          TIMESTAMPTZ NOT NULL,
    date_key                INTEGER     NOT NULL REFERENCES dim_date (date_key),
    product_key             INTEGER     NOT NULL REFERENCES dim_product (product_key),
    customer_key            INTEGER     NOT NULL REFERENCES dim_customer (customer_key),
    country_key             INTEGER     NOT NULL REFERENCES dim_country (country_key),
    quantity_returned       INTEGER     NOT NULL CHECK (quantity_returned >= 0),
    unit_price              NUMERIC(14,4) NOT NULL CHECK (unit_price > 0),
    return_amount           NUMERIC(16,4) NOT NULL,   -- cantidad devuelta x precio unitario
    is_cancellation_invoice BOOLEAN     NOT NULL,     -- la factura empieza por 'C'
    source_system           TEXT        NOT NULL,
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Neto por producto y dia: revenue bruto - devoluciones del mismo codigo en el mismo dia
CREATE TABLE IF NOT EXISTS agg_daily_product_net (
    date_key        INTEGER NOT NULL REFERENCES dim_date (date_key),
    product_key     INTEGER NOT NULL REFERENCES dim_product (product_key),
    units_sold      BIGINT        NOT NULL,
    units_returned  BIGINT        NOT NULL,
    gross_revenue   NUMERIC(18,4) NOT NULL,
    returns_amount  NUMERIC(18,4) NOT NULL,
    net_revenue     NUMERIC(18,4) NOT NULL,
    PRIMARY KEY (date_key, product_key)
);

-- Log de rechazos con motivo y fuente de origen
CREATE TABLE IF NOT EXISTS rejected_records (
    rejection_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_date         DATE        NOT NULL,
    source_system    TEXT        NOT NULL,
    source_row       INTEGER,                         -- numero de fila en el archivo original
    reject_reason    TEXT        NOT NULL,
    invoice_no_raw   TEXT,
    stock_code_raw   TEXT,
    payload          JSONB       NOT NULL,            -- fila original completa
    rejected_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_rejected_run_reason ON rejected_records (run_date, reject_reason);

-- Una fila por ejecucion (por fecha logica) con las metricas del pipeline
CREATE TABLE IF NOT EXISTS etl_run_summary (
    run_date     DATE PRIMARY KEY,
    metrics      JSONB       NOT NULL,
    finished_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_fact_sales_date    ON fact_sales (date_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_product ON fact_sales (product_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_country ON fact_sales (country_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_customer ON fact_sales (customer_key);
CREATE INDEX IF NOT EXISTS ix_fact_returns_date    ON fact_returns (date_key);
CREATE INDEX IF NOT EXISTS ix_fact_returns_product ON fact_returns (product_key);
