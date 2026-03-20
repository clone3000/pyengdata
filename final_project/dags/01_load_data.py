from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import glob
import os
import numpy as np

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def safe_datetime(value):
    if pd.isna(value) or value is None or value == 'NaT' or str(value) == 'NaT':
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value

def safe_str(value):
    if pd.isna(value) or value is None or str(value) == 'NaT':
        return None
    return str(value)

def safe_int(value):
    if pd.isna(value) or value is None:
        return None
    try:
        return int(value)
    except:
        return None

def safe_float(value):
    if pd.isna(value) or value is None:
        return None
    try:
        return float(value)
    except:
        return None

def load_parquet_files(**context):
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_delivery')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    tables = ['driver_history', 'order_items', 'orders', 'drivers', 'stores', 'users']
    for table in tables:
        cursor.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;")
    conn.commit()
    
    parquet_files = glob.glob('/opt/airflow/data/*.parquet')
    
    if not parquet_files:
        print(" Parquet файлы не найдены в /opt/airflow/data/")
        conn.close()
        return
    
    users_cache = {}
    stores_cache = {}
    drivers_cache = {}
    orders_cache = set()
    
    total_files = len(parquet_files)
    for idx, file_path in enumerate(parquet_files, 1):
        print(f"[{idx}/{total_files}] Обработка файла: {os.path.basename(file_path)}")
        df = pd.read_parquet(file_path)
        
        for _, row in df.iterrows():
            try:
                user_id = safe_int(row['user_id'])
                if user_id not in users_cache and user_id is not None:
                    cursor.execute("""
                        INSERT INTO users (user_id, user_phone) 
                        VALUES (%s, %s) 
                        ON CONFLICT (user_id) DO NOTHING
                    """, (user_id, safe_str(row['user_phone'])))
                    users_cache[user_id] = True
                
                store_id = safe_int(row['store_id'])
                if store_id not in stores_cache and store_id is not None:
                    cursor.execute("""
                        INSERT INTO stores (store_id, store_address) 
                        VALUES (%s, %s) 
                        ON CONFLICT (store_id) DO NOTHING
                    """, (store_id, safe_str(row['store_address'])))
                    stores_cache[store_id] = True
                
                driver_id = safe_int(row['driver_id'])
                if driver_id is not None and driver_id not in drivers_cache:
                    cursor.execute("""
                        INSERT INTO drivers (driver_id, driver_phone) 
                        VALUES (%s, %s) 
                        ON CONFLICT (driver_id) DO NOTHING
                    """, (driver_id, safe_str(row['driver_phone'])))
                    drivers_cache[driver_id] = True
                
                order_id = safe_int(row['order_id'])
                if order_id not in orders_cache and order_id is not None:
                    cursor.execute("""
                        INSERT INTO orders (
                            order_id, user_id, store_id, driver_id, address_text,
                            created_at, paid_at, delivery_started_at, delivered_at,
                            canceled_at, payment_type, order_discount,
                            order_cancellation_reason, delivery_cost
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (order_id) DO NOTHING
                    """, (
                        order_id,
                        safe_int(row['user_id']),
                        safe_int(row['store_id']),
                        safe_int(row['driver_id']),
                        safe_str(row['address_text']),
                        safe_datetime(row['created_at']),
                        safe_datetime(row['paid_at']),
                        safe_datetime(row['delivery_started_at']),
                        safe_datetime(row['delivered_at']),
                        safe_datetime(row['canceled_at']),
                        safe_str(row['payment_type']),
                        safe_float(row['order_discount']),
                        safe_str(row['order_cancellation_reason']),
                        safe_float(row['delivery_cost'])
                    ))
                    orders_cache.add(order_id)
                
                cursor.execute("""
                    INSERT INTO order_items (
                        order_id, item_id, item_title, item_category, item_quantity,
                        item_price, item_discount, item_canceled_quantity, replaced_by_item_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    safe_int(row['order_id']),
                    safe_int(row['item_id']),
                    safe_str(row['item_title']),
                    safe_str(row['item_category']),
                    safe_int(row['item_quantity']),
                    safe_float(row['item_price']),
                    safe_float(row['item_discount']),
                    safe_int(row['item_canceled_quantity']),
                    safe_int(row['item_replaced_id']) if 'item_replaced_id' in row and pd.notna(row['item_replaced_id']) else None
                ))
                
            except Exception as e:
                print(f" Ошибка при обработке строки: {e}")
                print(f"   Проблемная строка: order_id={row.get('order_id')}, item_id={row.get('item_id')}")
                continue
        
        conn.commit()
        print(f"+++ Файл {os.path.basename(file_path)} обработан")
    
    conn.close()
    print(" Загрузка всех данных завершена")

with DAG(
    '01_load_data',
    default_args=default_args,
    description='Загрузка Parquet файлов в нормализованные таблицы',
    schedule_interval=None,
    catchup=False,
    tags=['final_project']
) as dag:
    
    load_task = PythonOperator(
        task_id='load_parquet_to_postgres',
        python_callable=load_parquet_files
    )
    
    load_task
