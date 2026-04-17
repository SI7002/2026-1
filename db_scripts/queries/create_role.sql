DO $$
BEGIN
   IF NOT EXISTS (
      SELECT 1
      FROM pg_roles
      WHERE rolname = 'bookstore_app_rw'
   ) THEN
      CREATE ROLE bookstore_app_rw LOGIN PASSWORD 'Passwd123456!*';
   END IF;
END
$$;