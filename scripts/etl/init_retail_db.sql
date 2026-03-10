-- ============================================================
-- init_retail_db.sql
-- Location: scripts/etl/init_retail_db.sql
-- Mounted via docker-compose into /docker-entrypoint-initdb.d/
-- Runs automatically on first postgres container startup
-- ============================================================

CREATE DATABASE retail_db;

\c retail_db;

CREATE TABLE IF NOT EXISTS customers (
    customer_id  SERIAL PRIMARY KEY,
    name         VARCHAR(100)        NOT NULL,
    email        VARCHAR(100) UNIQUE NOT NULL,
    city         VARCHAR(50)         NOT NULL,
    created_at   TIMESTAMP           NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP           NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    product_id   SERIAL PRIMARY KEY,
    product_name VARCHAR(150) UNIQUE NOT NULL,
    category     VARCHAR(50)         NOT NULL,
    price        BIGINT              NOT NULL,
    created_at   TIMESTAMP           NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP           NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS purchase (
    purchase_id  SERIAL PRIMARY KEY,
    customer_id  INTEGER   NOT NULL REFERENCES customers(customer_id),
    product_id   INTEGER   NOT NULL REFERENCES products(product_id),
    quantity     SMALLINT  NOT NULL CHECK (quantity > 0),
    created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP NOT NULL DEFAULT NOW()
);
