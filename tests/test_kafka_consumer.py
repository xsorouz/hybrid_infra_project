# ============================
# 📥 CONSUMER DE TICKETS DEPUIS REDPANDA (Kafka-compatible)
#
# Ce script se connecte au broker Kafka (ici Redpanda), consomme les messages
# du topic 'client_tickets' et les affiche en temps réel afin de valider le flux
# de données ou pour le débogage.
# ============================

# ----------------------------
# 📦 IMPORTATION DES LIBRAIRIES
# ----------------------------
import json                         # Permet de désérialiser les messages JSON reçus en bytes.
import os                           # Fournit un accès aux variables d'environnement.
from kafka import KafkaConsumer     # Importation de KafkaConsumer pour consommer les messages depuis Kafka.
from loguru import logger           # Utilisé pour la journalisation, offrant des logs clairs et colorés.
from dotenv import load_dotenv      # Permet de charger les variables d'environnement à partir d'un fichier .env.

# ----------------------------
# 🔐 CHARGEMENT DES VARIABLES D'ENVIRONNEMENT
# ----------------------------
# Charge les configurations depuis le fichier .env, permettant d'éviter de coder en dur
# des informations sensibles telles que l'adresse du broker.
load_dotenv()

# Récupère l'adresse du broker Kafka depuis la variable d'environnement, sinon utilise une valeur par défaut.
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")  # Exemple : 'redpanda:9092' en environnement Docker.

# ----------------------------
# 🧲 INITIALISATION DU CONSUMER
# ----------------------------
# Crée une instance de KafkaConsumer qui se connecte au topic 'client_tickets'.
# Plusieurs paramètres sont configurés pour contrôler le comportement du consommateur.
consumer = KafkaConsumer(
    'client_tickets',                       # 🎯 Nom du topic à écouter.
    bootstrap_servers=KAFKA_BROKER,         # 🛰️ Adresse du broker Kafka, ici fournie par KAFKA_BROKER.
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),  # 🔓 Désérialisation des messages : convertit les bytes en chaîne de caractères, puis en objet JSON.
    auto_offset_reset='earliest',           # 🕰️ Commence la consommation depuis le plus ancien message disponible si aucun offset n'est présent.
    group_id='test-consumer-group',         # 👥 Définit le groupe de consommateurs, utile pour gérer le partitionnement et le partage des messages.
    enable_auto_commit=True                 # ✅ Permet le commit automatique des offsets, pour indiquer que les messages ont été traités.
)

# ----------------------------
# 🚦 MESSAGE D'ATTENTE
# ----------------------------
# Informe l'utilisateur que le consommateur est en attente de messages et prêt à les traiter.
logger.info("🟢 En attente de messages sur le topic 'client_tickets'... (Ctrl+C pour arrêter)")

# ----------------------------
# 🔄 BOUCLE DE CONSOMMATION
# ----------------------------
# Cette boucle itère sur les messages reçus du topic.
# Chaque message est traité dès qu'il est reçu et son contenu est affiché.
try:
    for message in consumer:
        # Affiche le contenu du message reçu dans le log avec le niveau de succès.
        logger.success("📩 Reçu : {}", message.value)
except KeyboardInterrupt:
    # Permet d'arrêter le consommateur proprement en cas d'interruption manuelle (Ctrl+C).
    logger.warning("🛑 Arrêt manuel (Ctrl+C)")
finally:
    # Ferme proprement la connexion au consommateur, libérant les ressources associées.
    consumer.close()
    logger.info("✅ Connexion au topic Kafka fermée proprement.")
