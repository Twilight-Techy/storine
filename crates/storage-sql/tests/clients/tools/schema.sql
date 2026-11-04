-- What every tool suite browses and queries.
CREATE TABLE customers (id SERIAL PRIMARY KEY, name VARCHAR(80) NOT NULL, email TEXT UNIQUE, region VARCHAR(10), created TIMESTAMP DEFAULT now(), active BOOLEAN DEFAULT true);
CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_id INT NOT NULL REFERENCES customers(id), amount NUMERIC(12,2) NOT NULL, status VARCHAR(20), ordered_at TIMESTAMP, note TEXT);
CREATE INDEX orders_customer ON orders (customer_id);
CREATE INDEX orders_status ON orders (status);
CREATE VIEW order_totals AS SELECT customer_id, count(*) AS n, sum(amount) AS total FROM orders GROUP BY customer_id;
INSERT INTO customers (name, email, region) VALUES ('Ada', 'ada@example.com', 'eu'), ('Bob', 'bob@example.com', 'us'), ('Cy', 'cy@example.com', 'eu'), ('Di', NULL, 'apac');
INSERT INTO orders (customer_id, amount, status, ordered_at) VALUES (1, 10.50, 'paid', '2024-01-01 10:00:00'), (1, 20.00, 'paid', '2024-02-01 10:00:00'), (2, 5.25, 'open', '2024-02-15 12:00:00'), (3, 100.00, 'paid', '2024-03-01 09:30:00'), (3, 7.75, 'void', '2024-03-02 09:30:00');
ANALYZE;
