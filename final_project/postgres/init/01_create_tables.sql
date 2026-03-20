CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    user_phone VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS stores (
    store_id INTEGER PRIMARY KEY,
    store_address TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS drivers (
    driver_id INTEGER PRIMARY KEY,
    driver_phone VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id),
    store_id INTEGER REFERENCES stores(store_id),
    driver_id INTEGER REFERENCES drivers(driver_id),
    address_text TEXT,
    created_at TIMESTAMP,
    paid_at TIMESTAMP,
    delivery_started_at TIMESTAMP,
    delivered_at TIMESTAMP,
    canceled_at TIMESTAMP,
    payment_type VARCHAR(50),
    order_discount NUMERIC(5,2),
    order_cancellation_reason TEXT,
    delivery_cost NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    item_id INTEGER,
    item_title VARCHAR(255),
    item_category VARCHAR(100),
    item_quantity INTEGER,
    item_price NUMERIC(10,2),
    item_discount NUMERIC(5,2),
    item_canceled_quantity INTEGER,
    replaced_by_item_id INTEGER REFERENCES order_items(order_item_id)
);

CREATE TABLE IF NOT EXISTS order_mart (
    year INTEGER,
    month INTEGER,
    day INTEGER,
    city VARCHAR(100),
    store_id INTEGER,
    turnover NUMERIC(15,2),
    revenue NUMERIC(15,2),
    orders_created INTEGER,
    orders_delivered INTEGER,
    orders_canceled INTEGER,
    unique_customers INTEGER
);

CREATE TABLE IF NOT EXISTS item_mart (
    year INTEGER,
    month INTEGER,
    day INTEGER,
    city VARCHAR(100),
    store_id INTEGER,
    category VARCHAR(100),
    item_id INTEGER,
    item_title VARCHAR(255),
    item_turnover NUMERIC(15,2),
    quantity_ordered INTEGER,
    quantity_canceled INTEGER,
    orders_with_item INTEGER,
    orders_with_cancel INTEGER
);