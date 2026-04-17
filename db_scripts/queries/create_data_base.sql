SELECT 'CREATE DATABASE bookstore'
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_database
    WHERE datname = 'bookstore'
)\gexec 