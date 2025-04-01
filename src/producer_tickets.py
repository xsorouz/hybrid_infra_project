# ============================
# 🚀 PRODUCTEUR DE TICKETS POUR REDPANDA (Kafka-compatible)
# Ce script génère des tickets fictifs avec Faker, et les envoie dans un topic Kafka via Redpanda.
# Utilisable pour des tests ou du streaming en continu.
# ============================

# 📦 JSON
# Permet de sérialiser les messages envoyés à Kafka au format JSON
import json

# 📦 TIME
# Pour gérer le délai entre chaque message envoyé (ex: time.sleep())
import time

# 📦 UUID
# Génère des identifiants uniques pour les tickets
import uuid

# 📦 RANDOM
# Pour choisir aléatoirement un type de requête ou une priorité
import random

# 📦 FAKER
# Générateur de données fictives réalistes (phrases, dates, ID client…)
from faker import Faker

# 📦 KAFKA PRODUCER
# Permet d’envoyer des messages dans un topic Kafka (ou Redpanda)
from kafka import KafkaProducer

# 📦 ARGPARSE
# Pour définir des arguments de ligne de commande (ex : --count, --interval)
import argparse

# 📦 LOGURU
# Logging moderne pour suivre l’envoi des messages
from loguru import logger

time.sleep(10)  # ⏱ Laisse Redpanda démarrer avant de se connecter

# ============================
# 🛠️ INITIALISATION DES OUTILS
# ============================

fake = Faker()

# Configuration du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Adresse du broker Redpanda
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Sérialisation JSON
)

# Types et niveaux de priorité disponibles (choix aléatoire)
REQUEST_TYPES = ['support', 'facturation', 'technique', 'commercial']
PRIORITIES = ['low', 'medium', 'high', 'urgent']

# ============================
# 📦 FONCTION : GÉNÉRATION D’UN TICKET ALÉATOIRE
# ============================
def generate_ticket():
    return {
        "ticket_id": str(uuid.uuid4()),  # Identifiant unique
        "client_id": fake.bothify(text='C#####'),  # ID client fictif
        "created_at": fake.iso8601(),  # Date ISO 8601
        "request": fake.sentence(nb_words=6),  # Phrase aléatoire
        "request_type": random.choice(REQUEST_TYPES),  # Catégorie de requête
        "priority": random.choice(PRIORITIES)  # Niveau de priorité
    }

# ============================
# 🚀 FONCTION : ENVOI EN BOUCLE DES TICKETS
# ============================
def produce_tickets(topic, total=None, interval=1.0):
    count = 0
    logger.info(f"📤 Envoi vers Redpanda (topic : '{topic}')... Ctrl+C pour arrêter.")
    try:
        while True:
            ticket = generate_ticket()
            producer.send(topic, value=ticket)  # Envoi du ticket
            logger.success("✅ Envoyé : {}", ticket)  # Log du ticket envoyé
            count += 1
            if total and count >= total:
                break  # Arrêt si limite atteinte
            time.sleep(interval)  # Pause entre deux tickets
    except KeyboardInterrupt:
        logger.warning("🛑 Arrêt demandé par l'utilisateur (Ctrl+C).")
    finally:
        producer.flush()  # Envoie les messages restants dans le buffer

# ============================
# 🧪 POINT D’ENTRÉE PRINCIPAL (CLI)
# Permet de personnaliser le script avec des arguments
# ============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="client_tickets", help="Nom du topic Kafka")
    parser.add_argument("--count", type=int, default=None, help="Nombre de tickets à envoyer (illimité si vide)")
    parser.add_argument("--interval", type=float, default=1.0, help="Intervalle entre tickets (en secondes)")
    args = parser.parse_args()

    produce_tickets(topic=args.topic, total=args.count, interval=args.interval)
