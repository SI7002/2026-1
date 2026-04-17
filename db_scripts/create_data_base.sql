-- =========================================================
-- 00_create_database.sql
-- Ejecutar conectado a la base "postgres"
-- Ejemplo:
-- psql -h <RDS_ENDPOINT> -U bookstore_admin -d postgres -f 00_create_database.sql
-- =========================================================

SELECT 'CREATE DATABASE bookstore'
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_database
    WHERE datname = 'bookstore'
)\gexec