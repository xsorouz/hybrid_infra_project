"""
Script de test de connexion à Redpanda via Kafka.
➡️ Utilise KafkaProducer pour envoyer un message dans un topic.
➡️ Se connecte à Redpanda (Kafka-compatible) exposé sur le port 9092.
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError
import time

# ✅ Définition du nom du topic et du message
topic = "iot_test"
message = b"Hello from Redpanda!"

# 🔁 Tentative de connexion avec délai si Redpanda n’est pas encore prêt
for attempt in range(10):
    try:
        # ✅ Création du producteur Kafka
        producer = KafkaProducer(bootstrap_servers='localhost:9092')  # 📡 Port Redpanda exposé
        print("✔ Connexion au broker Redpanda réussie")
        break
    except KafkaError as e:
        print(f"⏳ Tentative {attempt+1}/10 en attente de Redpanda...")
        time.sleep(1)
else:
    raise Exception("✘ Échec de la connexion à Redpanda après plusieurs tentatives.")

# ✉️ Envoi du message dans le topic spécifié
producer.send(topic, message)

# 💾 Forçage de l'envoi immédiat
producer.flush()

# ✅ Confirmation dans la console
print(f"✔ Message envoyé sur le topic '{topic}'")
