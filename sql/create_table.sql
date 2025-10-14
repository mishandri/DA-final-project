CREATE TABLE project.project_data (
    id SERIAL PRIMARY KEY,
    client_id INTEGER NOT NULL,
    gender VARCHAR(1),
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    price_per_item DECIMAL(10,2) NOT NULL CHECK (price_per_item >= 0),
    discount_per_item DECIMAL(10,2) DEFAULT 0 CHECK (discount_per_item >= 0),
    total_price DECIMAL(10,2) NOT NULL CHECK (total_price >= 0),
    purchase_datetime TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);