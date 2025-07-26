-- Source database setup
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    product_name VARCHAR(200),
    amount DECIMAL(10,2),
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO users (name, email) VALUES 
('John Doe', 'john@example.com'),
('Jane Smith', 'jane@example.com');

INSERT INTO orders (user_id, product_name, amount) VALUES 
(1, 'Laptop', 999.99),
(2, 'Phone', 699.99);

-- Create publication for logical replication
-- REPLICA IDENTITY tells PostgreSQL what information to include when replicating changes to other systems (like Debezium).
-- With REPLICA IDENTITY DEFAULT (only PK):
    -- {
    -- "op": "u",
    -- "before": {"id": 1},  // Only primary key
    -- "after": {"id": 1, "name": "John Updated", "email": "john@new.com"}
    -- }
-- With REPLICA IDENTITY FULL:
    -- {
    -- "op": "u", 
    -- "before": {"id": 1, "name": "John", "email": "john@old.com", "created_at": "2024-01-01"},
    -- "after": {"id": 1, "name": "John Updated", "email": "john@new.com", "created_at": "2024-01-01"}
    -- }
-- We use FULL to include all columns in the replication.
ALTER TABLE users REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
