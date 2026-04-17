-- =========================================================
-- 01_schema.sql
-- Ejecutar conectado a la base "bookstore"
-- Ejemplo:
-- psql -h <RDS_ENDPOINT> -U bookstore_admin -d bookstore -f 01_schema.sql
-- =========================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =========================================================
-- TABLA: books
-- =========================================================
CREATE TABLE IF NOT EXISTS books (
    book_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    isbn VARCHAR(20) UNIQUE,
    title VARCHAR(255) NOT NULL,
    author VARCHAR(255) NOT NULL,
    category VARCHAR(120),
    publisher VARCHAR(180),
    publication_year INTEGER,
    price NUMERIC(10,2) NOT NULL CHECK (price >= 0),
    stock INTEGER NOT NULL DEFAULT 0 CHECK (stock >= 0),
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

-- =========================================================
-- TABLA: customers
-- =========================================================
CREATE TABLE IF NOT EXISTS customers (
    customer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    first_name VARCHAR(120) NOT NULL,
    last_name VARCHAR(120) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    phone VARCHAR(30),
    city VARCHAR(120),
    country VARCHAR(120),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

-- =========================================================
-- TABLA: orders
-- =========================================================
CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NOT NULL,
    order_status VARCHAR(30) NOT NULL DEFAULT 'pending',
    total_amount NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (total_amount >= 0),
    order_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id)
);

-- =========================================================
-- TABLA: order_items
-- =========================================================
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id UUID NOT NULL,
    book_id UUID NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0),
    line_total NUMERIC(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_order_items_order
        FOREIGN KEY (order_id)
        REFERENCES orders(order_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_order_items_book
        FOREIGN KEY (book_id)
        REFERENCES books(book_id)
);

-- =========================================================
-- ÍNDICES
-- =========================================================
CREATE INDEX IF NOT EXISTS idx_books_category ON books(category);
CREATE INDEX IF NOT EXISTS idx_books_updated_at ON books(updated_at);

CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
CREATE INDEX IF NOT EXISTS idx_customers_updated_at ON customers(updated_at);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_timestamp ON orders(order_timestamp);
CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON orders(updated_at);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_book_id ON order_items(book_id);
CREATE INDEX IF NOT EXISTS idx_order_items_updated_at ON order_items(updated_at);

-- =========================================================
-- TRIGGER FUNCTION updated_at
-- =========================================================
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    t text;
BEGIN
    FOR t IN
        SELECT table_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND column_name = 'updated_at'
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS trg_updated_at ON %I', t);
        EXECUTE format(
            'CREATE TRIGGER trg_updated_at
             BEFORE UPDATE ON %I
             FOR EACH ROW
             EXECUTE FUNCTION set_updated_at()',
            t
        );
    END LOOP;
END $$;