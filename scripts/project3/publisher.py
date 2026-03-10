import json
import random
import time
import logging
from datetime import datetime
from google.cloud import pubsub_v1

# ===============================
# CONFIG
# ===============================

PROJECT_ID = "jcdeah-007"
TOPIC_ID = "retail-transaction-rakhajidhan"

CUSTOMERS = [
    {"customer_id": i + 1, "name": name, "city": city}
    for i, (name, city) in enumerate([
        ("Andi Saputra", "Jakarta"), ("Budi Santoso", "Bandung"),
        ("Citra Lestari", "Surabaya"), ("Dewi Anggraini", "Yogyakarta"),
        ("Eko Prasetyo", "Semarang"), ("Fajar Nugroho", "Medan"),
        ("Gita Permata", "Jakarta"), ("Hendra Wijaya", "Bandung"),
        ("Indah Kartika", "Surabaya"), ("Joko Susilo", "Jakarta"),
    ])
]

PRODUCTS = [
    {"product_id": 1, "product_name": "Laptop ASUS ROG", "category": "Electronics", "price": 18000000},
    {"product_id": 2, "product_name": "MacBook Air M1", "category": "Electronics", "price": 15000000},
    {"product_id": 3, "product_name": "iPhone 13 Pro", "category": "Electronics", "price": 14000000},
    {"product_id": 4, "product_name": "Samsung Galaxy S23", "category": "Electronics", "price": 13000000},
    {"product_id": 5, "product_name": "Sony Headphone WH-1000XM5", "category": "Electronics", "price": 5500000},
    {"product_id": 6, "product_name": "Logitech Wireless Mouse", "category": "Accessories", "price": 450000},
    {"product_id": 7, "product_name": "Mechanical Keyboard Keychron", "category": "Accessories", "price": 1200000},
    {"product_id": 8, "product_name": "Nike Air Jordan", "category": "Fashion", "price": 2500000},
    {"product_id": 9, "product_name": "Adidas Ultraboost", "category": "Fashion", "price": 2300000},
    {"product_id": 10, "product_name": "Uniqlo Hoodie", "category": "Fashion", "price": 600000},
]

PAYMENT_METHODS = ["credit_card", "debit_card", "bank_transfer", "e_wallet", "cash"]
STATUSES = ["completed", "pending", "cancelled"]


# ===============================
# PUBLISH FUNCTION
# ===============================

def publish_messages(num_messages: int = 20):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    logging.basicConfig(level=logging.INFO)
    logging.info(f"Publishing {num_messages} messages to {topic_path}")

    futures = []

    for i in range(num_messages):
        customer = random.choice(CUSTOMERS)
        product = random.choice(PRODUCTS)
        quantity = random.randint(1, 3)
        now = datetime.utcnow().isoformat()

        # Build a retail transaction event (related to Project 1 retail data)
        message = {
            "transaction_id": f"TXN-{int(time.time() * 1000)}-{i}",
            "event_type": "purchase",
            "customer_id": customer["customer_id"],
            "customer_name": customer["name"],
            "customer_city": customer["city"],
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "product_category": product["category"],
            "unit_price": product["price"],
            "quantity": quantity,
            "total_amount": product["price"] * quantity,
            "payment_method": random.choice(PAYMENT_METHODS),
            "status": random.choice(STATUSES),
            "published_at": now,
        }

        data = json.dumps(message).encode("utf-8")

        future = publisher.publish(
            topic_path,
            data=data,
            source="airflow-project3",
            event_type="retail_transaction",
        )
        futures.append((future, message["transaction_id"]))

        # Small delay to avoid flooding
        time.sleep(0.1)

    # Wait for all publishes to complete
    success_count = 0
    for future, txn_id in futures:
        try:
            msg_id = future.result(timeout=10)
            logging.info(f"Published {txn_id} → message_id={msg_id}")
            success_count += 1
        except Exception as e:
            logging.error(f"Failed to publish {txn_id}: {e}")

    logging.info(f"Done. {success_count}/{num_messages} messages published successfully.")
    return success_count


if __name__ == "__main__":
    publish_messages(num_messages=20)