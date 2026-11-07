-- NEXUS operational schema (Phase 1) + least-privilege roles
CREATE TABLE IF NOT EXISTS customers(customer_id SERIAL PRIMARY KEY, full_name TEXT NOT NULL, city TEXT NOT NULL, segment TEXT DEFAULT 'standard', created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS products(product_id SERIAL PRIMARY KEY, name TEXT NOT NULL, category TEXT NOT NULL, unit_price NUMERIC(10,2) NOT NULL, unit_cost NUMERIC(10,2) NOT NULL);
CREATE TABLE IF NOT EXISTS orders(order_id SERIAL PRIMARY KEY, customer_id INT REFERENCES customers, product_id INT REFERENCES products, city TEXT NOT NULL, quantity INT NOT NULL, unit_price NUMERIC(10,2) NOT NULL, status TEXT DEFAULT 'paid', created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS payments(payment_id SERIAL PRIMARY KEY, order_id INT REFERENCES orders, amount NUMERIC(10,2) NOT NULL, method TEXT, paid_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS inventory(product_id INT REFERENCES products, city TEXT, stock INT NOT NULL, updated_at TIMESTAMPTZ DEFAULT now(), PRIMARY KEY(product_id, city));
-- AI / API read-only role: Gold serving layer only in later phases
DO $$ BEGIN CREATE ROLE nexus_readonly LOGIN PASSWORD 'readonly_dev_only'; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO nexus_readonly;
