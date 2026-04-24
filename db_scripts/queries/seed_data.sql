-- =========================================================
-- 1) DATOS SEMILLA BASE
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

INSERT INTO customers (first_name, last_name, email, phone, city, country)
VALUES
('Juan','Perez','juan.perez@example.com','3001112233','Medellin','Colombia'),
('Ana','Gomez','ana.gomez@example.com','3002223344','Bogota','Colombia'),
('Carlos','Lopez','carlos.lopez@example.com','3003334455','Cali','Colombia')
ON CONFLICT (email) DO NOTHING;

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

-- =========================================================
-- 2) LIBROS SINTÉTICOS ADICIONALES
-- =========================================================

WITH synthetic_books AS (
    SELECT
        gs AS n,
        '9790000' || lpad(gs::text, 6, '0') AS isbn,
        CASE
            WHEN gs % 8 = 0 THEN 'Advanced SQL Patterns Vol. ' || gs
            WHEN gs % 8 = 1 THEN 'Cloud Data Pipelines Vol. ' || gs
            WHEN gs % 8 = 2 THEN 'Practical Analytics Vol. ' || gs
            WHEN gs % 8 = 3 THEN 'Distributed Systems Notes Vol. ' || gs
            WHEN gs % 8 = 4 THEN 'MLOps Foundations Vol. ' || gs
            WHEN gs % 8 = 5 THEN 'Data Modeling Handbook Vol. ' || gs
            WHEN gs % 8 = 6 THEN 'Applied Machine Learning Vol. ' || gs
            ELSE 'Software Architecture Review Vol. ' || gs
        END AS title,
        CASE
            WHEN gs % 8 = 0 THEN 'Author SQL ' || gs
            WHEN gs % 8 = 1 THEN 'Author Cloud ' || gs
            WHEN gs % 8 = 2 THEN 'Author Analytics ' || gs
            WHEN gs % 8 = 3 THEN 'Author Distributed ' || gs
            WHEN gs % 8 = 4 THEN 'Author MLOps ' || gs
            WHEN gs % 8 = 5 THEN 'Author Modeling ' || gs
            WHEN gs % 8 = 6 THEN 'Author ML ' || gs
            ELSE 'Author Software ' || gs
        END AS author,
        CASE
            WHEN gs % 8 = 0 THEN 'Databases'
            WHEN gs % 8 = 1 THEN 'Cloud Computing'
            WHEN gs % 8 = 2 THEN 'Analytics'
            WHEN gs % 8 = 3 THEN 'Distributed Systems'
            WHEN gs % 8 = 4 THEN 'MLOps'
            WHEN gs % 8 = 5 THEN 'Data Engineering'
            WHEN gs % 8 = 6 THEN 'Machine Learning'
            ELSE 'Software Engineering'
        END AS category,
        CASE
            WHEN gs % 5 = 0 THEN 'O Reilly'
            WHEN gs % 5 = 1 THEN 'Packt'
            WHEN gs % 5 = 2 THEN 'Manning'
            WHEN gs % 5 = 3 THEN 'MIT Press'
            ELSE 'Prentice Hall'
        END AS publisher,
        2012 + (gs % 13) AS publication_year,
        ROUND((25 + (gs % 70) + ((gs % 10) * 0.9))::numeric, 2) AS price,
        5 + (gs % 40) AS stock,
        'Synthetic book generated for bookstore ML/DataOps dataset' AS description
    FROM generate_series(1, 120) gs
)
INSERT INTO books (
    isbn, title, author, category, publisher, publication_year, price, stock, description
)
SELECT
    isbn, title, author, category, publisher, publication_year, price, stock, description
FROM synthetic_books
ON CONFLICT (isbn) DO NOTHING;

-- =========================================================
-- 3) CLIENTES SINTÉTICOS
-- =========================================================

WITH synthetic_customers AS (
    SELECT
        gs AS n,
        CASE
            WHEN gs % 10 = 0 THEN 'Sofia'
            WHEN gs % 10 = 1 THEN 'Mateo'
            WHEN gs % 10 = 2 THEN 'Valentina'
            WHEN gs % 10 = 3 THEN 'Santiago'
            WHEN gs % 10 = 4 THEN 'Isabella'
            WHEN gs % 10 = 5 THEN 'Daniel'
            WHEN gs % 10 = 6 THEN 'Camila'
            WHEN gs % 10 = 7 THEN 'Nicolas'
            WHEN gs % 10 = 8 THEN 'Mariana'
            ELSE 'Andres'
        END AS first_name,
        CASE
            WHEN gs % 10 = 0 THEN 'Ramirez'
            WHEN gs % 10 = 1 THEN 'Torres'
            WHEN gs % 10 = 2 THEN 'Castro'
            WHEN gs % 10 = 3 THEN 'Morales'
            WHEN gs % 10 = 4 THEN 'Vargas'
            WHEN gs % 10 = 5 THEN 'Herrera'
            WHEN gs % 10 = 6 THEN 'Jimenez'
            WHEN gs % 10 = 7 THEN 'Rojas'
            WHEN gs % 10 = 8 THEN 'Suarez'
            ELSE 'Mendoza'
        END AS last_name,
        'synthetic.customer.' || lpad(gs::text, 4, '0') || '@example.com' AS email,
        '300' || lpad((1000000 + gs)::text, 7, '0') AS phone,
        CASE
            WHEN gs % 6 = 0 THEN 'Medellin'
            WHEN gs % 6 = 1 THEN 'Bogota'
            WHEN gs % 6 = 2 THEN 'Cali'
            WHEN gs % 6 = 3 THEN 'Barranquilla'
            WHEN gs % 6 = 4 THEN 'Bucaramanga'
            ELSE 'Cartagena'
        END AS city,
        'Colombia' AS country
    FROM generate_series(1, 400) gs
)
INSERT INTO customers (
    first_name, last_name, email, phone, city, country
)
SELECT
    first_name, last_name, email, phone, city, country
FROM synthetic_customers
ON CONFLICT (email) DO NOTHING;

-- =========================================================
-- 4) ÓRDENES Y ORDER_ITEMS SINTÉTICOS
-- =========================================================

DO $$
DECLARE
    rec_customer RECORD;
    v_segment TEXT;
    v_orders_to_create INT;
    v_i INT;
    v_order_id UUID;
    v_order_ts TIMESTAMPTZ;
    v_items_count INT;
    v_book RECORD;
    v_total NUMERIC(10,2);
    v_status TEXT;
    v_existing_orders INT;
BEGIN
    PERFORM setseed(0.42);

    FOR rec_customer IN
        SELECT
            c.customer_id,
            c.email,
            row_number() OVER (ORDER BY c.email) AS rn
        FROM customers c
        WHERE c.email LIKE 'synthetic.customer.%@example.com'
        ORDER BY c.email
    LOOP
        SELECT COUNT(*)
        INTO v_existing_orders
        FROM orders o
        WHERE o.customer_id = rec_customer.customer_id;

        IF v_existing_orders > 0 THEN
            CONTINUE;
        END IF;

        IF rec_customer.rn <= 120 THEN
            v_segment := 'active';
            v_orders_to_create := 8 + floor(random() * 10)::int;
        ELSIF rec_customer.rn <= 240 THEN
            v_segment := 'at_risk';
            v_orders_to_create := 4 + floor(random() * 6)::int;
        ELSIF rec_customer.rn <= 340 THEN
            v_segment := 'churned';
            v_orders_to_create := 2 + floor(random() * 4)::int;
        ELSE
            v_segment := 'new_low_activity';
            v_orders_to_create := 1 + floor(random() * 3)::int;
        END IF;

        FOR v_i IN 1..v_orders_to_create LOOP
            IF v_segment = 'active' THEN
                v_order_ts := now() - ((random() * 90)::int || ' days')::interval
                                   - ((random() * 23)::int || ' hours')::interval;
            ELSIF v_segment = 'at_risk' THEN
                v_order_ts := now() - ((90 + random() * 120)::int || ' days')::interval
                                   - ((random() * 23)::int || ' hours')::interval;
            ELSIF v_segment = 'churned' THEN
                v_order_ts := now() - ((220 + random() * 300)::int || ' days')::interval
                                   - ((random() * 23)::int || ' hours')::interval;
            ELSE
                v_order_ts := now() - ((random() * 45)::int || ' days')::interval
                                   - ((random() * 23)::int || ' hours')::interval;
            END IF;

            v_status := CASE
                WHEN random() < 0.80 THEN 'paid'
                WHEN random() < 0.90 THEN 'completed'
                WHEN random() < 0.96 THEN 'shipped'
                ELSE 'cancelled'
            END;

            INSERT INTO orders (
                customer_id,
                order_status,
                total_amount,
                order_timestamp,
                created_at,
                updated_at
            )
            VALUES (
                rec_customer.customer_id,
                v_status,
                0,
                v_order_ts,
                v_order_ts,
                v_order_ts
            )
            RETURNING order_id INTO v_order_id;

            v_items_count := 1 + floor(random() * 3)::int;
            v_total := 0;

            FOR v_book IN
                SELECT
                    b.book_id,
                    b.price
                FROM books b
                ORDER BY random()
                LIMIT v_items_count
            LOOP
                INSERT INTO order_items (
                    order_id,
                    book_id,
                    quantity,
                    unit_price,
                    created_at,
                    updated_at
                )
                VALUES (
                    v_order_id,
                    v_book.book_id,
                    1 + floor(random() * 2)::int,
                    v_book.price,
                    v_order_ts,
                    v_order_ts
                );

                SELECT COALESCE(SUM(line_total), 0)
                INTO v_total
                FROM order_items
                WHERE order_id = v_order_id;
            END LOOP;

            UPDATE orders
            SET total_amount = v_total,
                updated_at = v_order_ts
            WHERE order_id = v_order_id;

        END LOOP;
    END LOOP;
END $$;

-- =========================================================
-- 5) ALGUNOS CLIENTES REALES CON MÁS HISTORIA
-- =========================================================

DO $$
DECLARE
    rec_customer RECORD;
    v_idx INT := 0;
    v_order_id UUID;
    v_order_ts TIMESTAMPTZ;
    v_book RECORD;
    v_total NUMERIC(10,2);
BEGIN
    FOR rec_customer IN
        SELECT customer_id, email
        FROM customers
        WHERE email IN (
            'juan.perez@example.com',
            'ana.gomez@example.com',
            'carlos.lopez@example.com'
        )
    LOOP
        IF (
            SELECT COUNT(*)
            FROM orders o
            WHERE o.customer_id = rec_customer.customer_id
        ) < 6 THEN
            FOR v_idx IN 1..6 LOOP
                v_order_ts := now() - ((20 * v_idx + floor(random() * 10))::text || ' days')::interval;

                INSERT INTO orders (
                    customer_id,
                    order_status,
                    total_amount,
                    order_timestamp,
                    created_at,
                    updated_at
                )
                VALUES (
                    rec_customer.customer_id,
                    CASE WHEN random() < 0.9 THEN 'paid' ELSE 'completed' END,
                    0,
                    v_order_ts,
                    v_order_ts,
                    v_order_ts
                )
                RETURNING order_id INTO v_order_id;

                v_total := 0;

                FOR v_book IN
                    SELECT b.book_id, b.price
                    FROM books b
                    ORDER BY random()
                    LIMIT (1 + floor(random() * 2)::int)
                LOOP
                    INSERT INTO order_items (
                        order_id,
                        book_id,
                        quantity,
                        unit_price,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        v_order_id,
                        v_book.book_id,
                        1,
                        v_book.price,
                        v_order_ts,
                        v_order_ts
                    );

                    v_total := v_total + v_book.price;
                END LOOP;

                UPDATE orders
                SET total_amount = v_total
                WHERE order_id = v_order_id;
            END LOOP;
        END IF;
    END LOOP;
END $$;