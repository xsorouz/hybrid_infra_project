# ============================
# 📥 CONSUMER DE TICKETS DEPUIS REDPANDA (Kafka-compatible)
# Ce script écoute un topic Kafka (via Redpanda) et affiche en temps réel
# les tickets clients reçus au format JSON.
# ============================

# 📦 KAFKA
# KafkaConsumer permet de se connecter à un topic Kafka et de consommer les messages
from kafka import KafkaConsumer

# 📦 LOGURU
# Librairie de logging moderne et élégante (niveau, timestamp, couleurs)
from loguru import logger

# 📦 JSON
# Permet de désérialiser les messages JSON reçus de Kafka
import json

# ============================
# 🧲 INITIALISATION DU CONSUMER
# Connexion au broker Kafka et configuration de la consommation
# ============================

consumer = KafkaConsumer(
    'client_tickets',  # 🎯 Nom du topic à écouter (doit être identique à celui utilisé par le producteur)
    bootstrap_servers='localhost:9092',  # 🛰️ Adresse de Redpanda ou Kafka
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),  # 🔓 Décodage JSON automatique
    auto_offset_reset='earliest',  # 🕰️ Reprend à partir du plus ancien message disponible
    group_id='test-consumer-group',  # 👥 Groupe de consommation (pour suivre la progression)
    enable_auto_commit=True  # ✅ Commit automatique des offsets (conservation de l’état)
)

# ============================
# 🚦 MESSAGE D’ATTENTE
# ============================

logger.info("🟢 En attente de messages sur le topic 'client_tickets'... (Ctrl+C pour arrêter)")

# ============================
# 🔄 BOUCLE DE CONSOMMATION
# Affiche chaque message reçu en temps réel
# ============================

try:
    for message in consumer:
        logger.success("📩 Reçu : {}", message.value)  # 📨 Affiche le contenu du ticket reçu
except KeyboardInterrupt:
    logger.warning("🛑 Arrêt manuel du consommateur.")  # ⚠️ Arrêt propre du consumer avec Ctrl+C
