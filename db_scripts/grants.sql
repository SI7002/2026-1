-- =========================================================
-- 03_grants.sql
-- Ejecutar conectado a la base "bookstore"
-- con el master user de RDS
-- =========================================================

DO $$
BEGIN
   IF NOT EXISTS (
      SELECT 1
      FROM pg_roles
      WHERE rolname = 'bookstore_app'
   ) THEN
      CREATE ROLE bookstore_app LOGIN PASSWORD 'ChangeThisNow_2026!';
   END IF;
END
$$;

GRANT CONNECT ON DATABASE bookstore TO bookstore_app;
GRANT USAGE ON SCHEMA public TO bookstore_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO bookstore_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO bookstore_app;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO bookstore_app;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT USAGE, SELECT ON SEQUENCES TO bookstore_app;