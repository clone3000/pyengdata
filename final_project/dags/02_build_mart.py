from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def build_order_mart():
    pg_hook = PostgresHook(postgres_conn_id='postgres_delivery')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("TRUNCATE TABLE order_mart;")
    
    cursor.execute("""
        INSERT INTO order_mart (
            year, month, day, city, store_id,
            turnover, revenue, orders_created, orders_delivered,
            orders_canceled, unique_customers
        )
        SELECT 
            EXTRACT(YEAR FROM o.created_at) as year,
            EXTRACT(MONTH FROM o.created_at) as month,
            EXTRACT(DAY FROM o.created_at) as day,
            SPLIT_PART(o.address_text, ',', 1) as city,
            o.store_id,
            SUM(oi.item_price * oi.item_quantity) as turnover,
            SUM(oi.item_price * oi.item_quantity * (1 - COALESCE(oi.item_discount, 0)/100) * (1 - COALESCE(o.order_discount, 0)/100)) as revenue,
            COUNT(DISTINCT o.order_id) as orders_created,
            COUNT(DISTINCT CASE WHEN o.delivered_at IS NOT NULL THEN o.order_id END) as orders_delivered,
            COUNT(DISTINCT CASE WHEN o.canceled_at IS NOT NULL THEN o.order_id END) as orders_canceled,
            COUNT(DISTINCT o.user_id) as unique_customers
        FROM orders o
        LEFT JOIN order_items oi ON o.order_id = oi.order_id
        GROUP BY year, month, day, city, o.store_id
    """)
    
    conn.commit()
    cursor.close()
    conn.close()
    print(" Витрина заказов построена")

def build_item_mart():
    pg_hook = PostgresHook(postgres_conn_id='postgres_delivery')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("TRUNCATE TABLE item_mart;")
    
    cursor.execute("""
        INSERT INTO item_mart (
            year, month, day, city, store_id,
            category, item_id, item_title,
            item_turnover, quantity_ordered, quantity_canceled,
            orders_with_item, orders_with_cancel
        )
        SELECT 
            EXTRACT(YEAR FROM o.created_at) as year,
            EXTRACT(MONTH FROM o.created_at) as month,
            EXTRACT(DAY FROM o.created_at) as day,
            SPLIT_PART(o.address_text, ',', 1) as city,
            o.store_id,
            oi.item_category as category,
            oi.item_id,
            oi.item_title,
            SUM(oi.item_price * oi.item_quantity * (1 - COALESCE(oi.item_discount, 0)/100) * (1 - COALESCE(o.order_discount, 0)/100)) as item_turnover,
            SUM(oi.item_quantity) as quantity_ordered,
            SUM(oi.item_canceled_quantity) as quantity_canceled,
            COUNT(DISTINCT o.order_id) as orders_with_item,
            COUNT(DISTINCT CASE WHEN oi.item_canceled_quantity > 0 THEN o.order_id END) as orders_with_cancel
        FROM orders o
        LEFT JOIN order_items oi ON o.order_id = oi.order_id
        GROUP BY year, month, day, city, o.store_id, oi.item_category, oi.item_id, oi.item_title
    """)
    
    conn.commit()
    cursor.close()
    conn.close()
    print(" Витрина товаров построена")

with DAG(
    '02_build_mart',
    default_args=default_args,
    description='Построение витрин через SQL',
    schedule_interval=None,
    catchup=False,
    tags=['final_project']
) as dag:
    
    build_order = PythonOperator(
        task_id='build_order_mart',
        python_callable=build_order_mart
    )
    
    build_item = PythonOperator(
        task_id='build_item_mart',
        python_callable=build_item_mart
    )
    
    build_order >> build_item