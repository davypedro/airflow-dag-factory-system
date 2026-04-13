-- ============================================================
--  DDL — Data Warehouse (desenvolvimento / Docker)
-- ============================================================
--  Executado automaticamente pelo postgres na primeira vez
--  que o container sobe (via docker-entrypoint-initdb.d).
--
--  Todas as tabelas usam CREATE TABLE IF NOT EXISTS para que
--  re-execuções sejam idempotentes.
-- ============================================================

-- ── Dimensão: Produtos ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.dim_products (
    product_id      INTEGER         PRIMARY KEY,
    product_name    VARCHAR(255)    NOT NULL,
    sku             VARCHAR(50)     NOT NULL UNIQUE,
    category        VARCHAR(100),
    price           NUMERIC(12, 2),
    description     TEXT,
    _etl_loaded_at  TIMESTAMP       DEFAULT NOW()
);

-- ── Dimensão: Clientes ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.dim_customers (
    customer_id         INTEGER         PRIMARY KEY,
    full_name           VARCHAR(255)    NOT NULL,
    email               VARCHAR(255),
    phone               VARCHAR(30),
    city                VARCHAR(100),
    state               CHAR(2),
    zip_code            VARCHAR(10),
    segment             VARCHAR(50),
    registration_date   TIMESTAMP,
    is_active           BOOLEAN,
    _etl_loaded_at      TIMESTAMP       DEFAULT NOW()
);

-- ── Dimensão: Estoque ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.dim_inventory (
    product_id          INTEGER         PRIMARY KEY,
    sku                 VARCHAR(50)     NOT NULL,
    stock_quantity      INTEGER         NOT NULL DEFAULT 0,
    reserved_quantity   INTEGER         NOT NULL DEFAULT 0,
    warehouse           VARCHAR(50),
    last_restock_date   DATE,
    restock_threshold   INTEGER,
    unit_cost           NUMERIC(12, 2),
    _etl_loaded_at      TIMESTAMP       DEFAULT NOW()
);

-- ── Fato: Pedidos ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.fact_orders (
    order_id            VARCHAR(20)     PRIMARY KEY,
    customer_id         INTEGER,
    order_date          TIMESTAMP,
    status              VARCHAR(20),
    total_amount        NUMERIC(12, 2),
    discount_amount     NUMERIC(12, 2)  DEFAULT 0,
    shipping_amount     NUMERIC(12, 2)  DEFAULT 0,
    payment_method      VARCHAR(30),
    notes               TEXT,
    net_amount          NUMERIC(12, 2),
    _etl_loaded_at      TIMESTAMP       DEFAULT NOW()
);

-- ── Fato: Itens de Pedido ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.fact_order_items (
    item_id         INTEGER         PRIMARY KEY,
    order_id        VARCHAR(20),
    product_id      INTEGER,
    quantity        INTEGER         NOT NULL,
    unit_price      NUMERIC(12, 2)  NOT NULL,
    discount_pct    NUMERIC(5, 2)   DEFAULT 0,
    line_total      NUMERIC(12, 2),
    _etl_loaded_at  TIMESTAMP       DEFAULT NOW()
);
