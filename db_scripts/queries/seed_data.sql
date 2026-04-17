-- =========================================================
-- 02_seed_data.sql
-- Ejecutar conectado a la base "bookstore"
-- =========================================================

-- =========================================================
-- DATOS SEMILLA: books
-- =========================================================
INSERT INTO books (isbn, title, author, category, publisher, publication_year, price, stock, description)
VALUES
('9780131103627','The C Programming Language','Brian W. Kernighan','Programming','Prentice Hall',1988,45.00,20,'Classic book on C programming'),
('9780132350884','Clean Code','Robert C. Martin','Software Engineering','Prentice Hall',2008,52.00,15,'Best practices for writing clean code'),
('9781491950357','Designing Data-Intensive Applications','Martin Kleppmann','Data Engineering','O Reilly',2017,65.00,10,'Modern systems and data architecture'),
('9781098104030','Fundamentals of Data Engineering','Joe Reis','Data Engineering','O Reilly',2022,59.00,12,'Foundations of data engineering'),
('9780262046305','Introduction to Algorithms','Thomas H. Cormen','Algorithms','MIT Press',2022,80.00,8,'Algorithms reference book'),
('9781617294433','Spring in Action','Craig Walls','Programming','Manning',2022,55.00,9,'Guide to Spring framework'),
('9781492078005','Machine Learning Design Patterns','Valliappa Lakshmanan','Machine Learning','O Reilly',2020,62.00,7,'Reusable ML design patterns'),
('9781789801817','Hands-On MLOps','Noah Gift','MLOps','Packt',2021,48.00,11,'Practical MLOps guide')
ON CONFLICT (isbn) DO NOTHING;

-- =========================================================
-- DATOS SEMILLA: customers
-- =========================================================
INSERT INTO customers (first_name, last_name, email, phone, city, country)
VALUES
('Juan','Perez','juan.perez@example.com','3001112233','Medellin','Colombia'),
('Ana','Gomez','ana.gomez@example.com','3002223344','Bogota','Colombia'),
('Carlos','Lopez','carlos.lopez@example.com','3003334455','Cali','Colombia')
ON CONFLICT (email) DO NOTHING;

-- =========================================================
-- PEDIDO DE EJEMPLO
-- Evita duplicar el pedido si ejecutas el seed varias veces
-- =========================================================
DO $$
DECLARE
    v_customer_id UUID;
    v_order_id UUID;
    v_book1 UUID;
    v_book2 UUID;
    v_order_exists BOOLEAN;
BEGIN
    SELECT customer_id INTO v_customer_id
    FROM customers
    WHERE email = 'juan.perez@example.com';

    SELECT book_id INTO v_book1
    FROM books
    WHERE isbn = '9781491950357';

    SELECT book_id INTO v_book2
    FROM books
    WHERE isbn = '9781098104030';

    SELECT EXISTS (
        SELECT 1
        FROM orders o
        JOIN customers c ON c.customer_id = o.customer_id
        WHERE c.email = 'juan.perez@example.com'
          AND o.total_amount = 124.00
          AND o.order_status = 'paid'
    ) INTO v_order_exists;

    IF NOT v_order_exists
       AND v_customer_id IS NOT NULL
       AND v_book1 IS NOT NULL
       AND v_book2 IS NOT NULL THEN

        INSERT INTO orders (customer_id, order_status, total_amount)
        VALUES (v_customer_id, 'paid', 124.00)
        RETURNING order_id INTO v_order_id;

        INSERT INTO order_items (order_id, book_id, quantity, unit_price)
        VALUES
        (v_order_id, v_book1, 1, 65.00),
        (v_order_id, v_book2, 1, 59.00);
    END IF;
END $$;