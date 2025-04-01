"""
Script global de test de l’infrastructure hybride Docker :
- Redpanda (Kafka)
- MinIO (S3)
- ClickHouse (analytique)
- PostgreSQL (relationnelle)
"""

import os
import time
import io
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
from minio import Minio
from minio.error import S3Error
from clickhouse_driver import Client
import psycopg2
from psycopg2 import OperationalError

# ✅ Chargement des variables d’environnement depuis .env
load_dotenv()


# === Test Redpanda (Kafka) ===
print("\n=== Test Redpanda ===")
try:
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    producer.send('iot_test', b'Hello from Redpanda!')
    producer.flush()
    print("✔ Message envoyé dans Redpanda")
except KafkaError as e:
    print("✘ Redpanda échoué :", e)
except Exception as e:
    print("✘ Redpanda erreur inconnue :", e)


# === Test MinIO ===
print("\n=== Test MinIO ===")
try:
    minio_client = Minio(
        "localhost:9000",
        access_key=os.getenv("MINIO_ROOT_USER", "admin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "admin123"),
        secure=False
    )

    bucket = "test-bucket"
    obj_name = "hello.txt"
    content = b"Hello from MinIO!"

    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)
        print(f"✔ Bucket '{bucket}' créé")
    else:
        print(f"✔ Bucket '{bucket}' déjà existant")

    minio_client.put_object(bucket, obj_name, io.BytesIO(content), length=len(content))
    print(f"✔ Fichier '{obj_name}' envoyé dans le bucket '{bucket}'")
except S3Error as e:
    print("✘ MinIO échoué :", e)
except Exception as e:
    print("✘ MinIO erreur inconnue :", e)


# === Test ClickHouse ===
print("\n=== Test ClickHouse ===")
try:
    click_client = Client(
        host='localhost',
        port=9009,
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "")
    )
    click_client.execute("CREATE DATABASE IF NOT EXISTS testdb")
    click_client.execute("""
        CREATE TABLE IF NOT EXISTS testdb.hello (
            id UInt32,
            msg String
        ) ENGINE = Memory
    """)
    click_client.execute("INSERT INTO testdb.hello VALUES (1, 'Hello from ClickHouse!')")
    rows = click_client.execute("SELECT * FROM testdb.hello")
    print("✔ Résultat ClickHouse :", rows)
except Exception as e:
    print("✘ ClickHouse échoué :", e)


# === Test PostgreSQL ===
print("\n=== Test PostgreSQL ===")
try:
    # ⏳ Tentatives de connexion (10 fois max, 1 sec d’attente entre)
    for i in range(10):
        try:
            conn = psycopg2.connect(
                dbname=os.getenv("POSTGRES_DB", "demo_db"),
                user=os.getenv("POSTGRES_USER", "user"),
                password=os.getenv("POSTGRES_PASSWORD", "pass"),
                host="localhost",
                port=5432
            )
            print("✔ Connexion à PostgreSQL réussie")
            break
        except OperationalError:
            print(f"⏳ Tentative {i+1}/10 en attente de PostgreSQL...")
            time.sleep(1)
    else:
        raise Exception("✘ PostgreSQL n’a pas démarré à temps.")

    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS greetings (
            id SERIAL PRIMARY KEY,
            message TEXT
        );
    """)
    cur.execute("INSERT INTO greetings (message) VALUES (%s)", ("Hello from PostgreSQL!",))
    conn.commit()
    cur.execute("SELECT * FROM greetings;")
    rows = cur.fetchall()
    print("✔ Résultat PostgreSQL :", rows)

    cur.close()
    conn.close()

except Exception as e:
    print("✘ PostgreSQL échoué :", e)
