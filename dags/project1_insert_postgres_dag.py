from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
import psycopg2
import random


# -------------------------------
# DATABASE CONFIG
# Matches docker-compose.yml:
#   host: postgres (service name)
#   port: 5432 (internal)
#   user/password: airflow/airflow
#   database: retail_db (separate from airflow metadata db)
# -------------------------------

DB_CONFIG = {
    "host":     "postgres",
    "port":     5432,
    "database": "retail_db",
    "user":     "airflow",
    "password": "airflow",
}


# -------------------------------
# DATA GENERATOR
# -------------------------------

CUSTOMERS = [
    "Andi Saputra", "Budi Santoso", "Citra Lestari", "Dewi Anggraini",
    "Eko Prasetyo", "Fajar Nugroho", "Gita Permata", "Hendra Wijaya",
    "Indah Kartika", "Joko Susilo", "Kartika Putri", "Lukman Hakim",
    "Maya Sari", "Nanda Pratama", "Oktavia Putri", "Putra Aditya",
    "Rina Wulandari", "Satria Mahendra", "Tina Kurniawati", "Yusuf Ramadhan",
]

CITIES = ["Jakarta", "Bandung", "Surabaya", "Yogyakarta", "Semarang", "Medan"]

PRODUCTS = [
    ("Laptop ASUS ROG",              "Electronics",  18_000_000),
    ("MacBook Air M1",               "Electronics",  15_000_000),
    ("iPhone 13 Pro",                "Electronics",  14_000_000),
    ("Samsung Galaxy S23",           "Electronics",  13_000_000),
    ("Sony Headphone WH-1000XM5",    "Electronics",   5_500_000),
    ("Logitech Wireless Mouse",      "Accessories",     450_000),
    ("Mechanical Keyboard Keychron", "Accessories",   1_200_000),
    ("Nike Air Jordan",              "Fashion",       2_500_000),
    ("Adidas Ultraboost",            "Fashion",       2_300_000),
    ("Uniqlo Hoodie",                "Fashion",         600_000),
    ("Levi's Jeans",                 "Fashion",         900_000),
    ("Xiaomi Smart TV 43",           "Electronics",   5_200_000),
    ("Canon EOS M50 Camera",         "Electronics",   8_900_000),
    ("Apple Watch Series 8",         "Electronics",   7_800_000),
    ("Samsung Galaxy Buds",          "Electronics",   2_200_000),
]


# -------------------------------
# HELPERS
# -------------------------------

def get_or_create_customer(cur, now):
    name  = random.choice(CUSTOMERS)
    email = name.replace(" ", "").lower() + "@mail.com"
    city  = random.choice(CITIES)

    cur.execute("SELECT customer_id FROM customers WHERE email = %s", (email,))
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        """
        INSERT INTO customers (name, email, city, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING customer_id
        """,
        (name, email, city, now, now),
    )
    return cur.fetchone()[0]


def get_or_create_product(cur, now):
    product_name, category, price = random.choice(PRODUCTS)

    cur.execute("SELECT product_id FROM products WHERE product_name = %s", (product_name,))
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        """
        INSERT INTO products (product_name, category, price, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING product_id
        """,
        (product_name, category, price, now, now),
    )
    return cur.fetchone()[0]


# -------------------------------
# MAIN TASK FUNCTION
# -------------------------------

def insert_retail_data():
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()
    now  = datetime.now()

    try:
        customer_id = get_or_create_customer(cur, now)
        product_id  = get_or_create_product(cur, now)
        quantity    = random.randint(1, 3)

        cur.execute(
            """
            INSERT INTO purchase (customer_id, product_id, quantity, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (customer_id, product_id, quantity, now, now),
        )

        conn.commit()
        print(f"[OK] Inserted purchase: customer_id={customer_id}, product_id={product_id}, qty={quantity}")

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cur.close()
        conn.close()


# -------------------------------
# AIRFLOW DAG
# -------------------------------

default_args = {
    "owner": "rakha",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="project1_insert_retail_postgres",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
    tags=["project1", "retail", "postgres"],
    description="Hourly insert of retail transactions into PostgreSQL",
) as dag:

    insert_retail = PythonOperator(
        task_id="insert_retail_transaction",
        python_callable=insert_retail_data,
    )

    insert_retail