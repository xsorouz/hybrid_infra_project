# ==============================================================================
# 👁️ CONSUMER DE TICKETS KAFKA (Redpanda)
#
# Ce script se connecte à un broker Kafka-compatible (ici Redpanda),
# puis écoute en temps réel les messages du topic "client_tickets".
# Il affiche les tickets reçus un par un dans la console.
#
# Ce consumer est utile pour :
# - Vérifier que les tickets sont bien publiés sur le broker.
# - Déboguer un pipeline de streaming Kafka/Redpanda.
# - Intégrer une lecture automatisée dans un autre système.
# ==============================================================================

# ==============================================================================
# 📦 IMPORTATION DES LIBRAIRIES
# ==============================================================================
# --- Librairies standards ---
import os                        # Pour accéder aux variables d’environnement définies dans le système ou dans un fichier .env
import json                      # Pour décoder et manipuler les messages encodés en JSON

# --- Librairies externes ---
from kafka import KafkaConsumer             # Fournit la classe principale pour consommer des messages depuis Kafka
from kafka.errors import NoBrokersAvailable  # Exception levée lorsqu'aucun broker n'est disponible pour la connexion
from dotenv import load_dotenv              # Permet de charger les variables d’environnement à partir d’un fichier .env
from loguru import logger                   # Outil de logging pour un affichage clair, coloré et détaillé des messages de log

# ==============================================================================
# 🔐 CHARGEMENT DES VARIABLES D'ENVIRONNEMENT
# ==============================================================================
# Charge les variables d’environnement depuis le fichier .env (situé à la racine du projet)
load_dotenv()

# Récupère la configuration du broker Kafka et du topic depuis les variables d’environnement.
# Des valeurs par défaut sont fournies pour faciliter les tests en environnement de développement.
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")  # Adresse du broker Kafka (nom du conteneur et port)
TOPIC = os.getenv("KAFKA_TOPIC", "client_tickets")         # Nom du topic à écouter pour recevoir les tickets

# ==============================================================================
# 🚦 CONNEXION À REDPANDA (KAFKA BROKER)
# ==============================================================================
logger.info("⏳ Connexion à Redpanda (Kafka-compatible)...")

# Tentative de connexion répétée afin de gérer le cas où Redpanda ne serait pas encore opérationnel.
# La boucle essaie de se connecter jusqu'à 20 fois (avec une pause entre chaque tentative).
for attempt in range(20):
    try:
        # Initialisation du consumer Kafka avec les paramètres suivants :
        # - TOPIC : le nom du topic à écouter.
        # - bootstrap_servers : l'adresse du broker Kafka.
        # - auto_offset_reset : permet de lire tous les messages depuis le début (utile en phase de test ou débogage).
        # - enable_auto_commit : active la validation automatique de la position de lecture dans le topic.
        # - group_id : identifiant du groupe de consommateurs pour gérer le partage de messages.
        # - value_deserializer : fonction qui décode le message reçu du format binaire en JSON.
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="ticket-consumer-group",
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        # Si la connexion réussit, on sort de la boucle
        logger.success("✅ Connecté au broker Kafka.")
        break

    except NoBrokersAvailable:
        # Si la connexion échoue, on affiche un message d'avertissement avec le numéro de tentative
        logger.warning(f"🔁 Tentative {attempt + 1}/20... Redpanda non encore prêt.")
        import time
        time.sleep(3)  # Pause de 3 secondes avant de retenter la connexion

# Si la connexion échoue après 20 tentatives (~60 secondes), le script s'arrête.
else:
    logger.error("❌ Impossible de se connecter à Redpanda après 60 secondes.")
    exit(1)

# ==============================================================================
# 📥 ÉCOUTE DU TOPIC ET AFFICHAGE DES MESSAGES
# ==============================================================================
logger.info(f"🎧 En écoute sur le topic : '{TOPIC}'... (Appuyez sur Ctrl+C pour quitter)")

try:
    # Boucle infinie qui lit les messages à mesure qu'ils arrivent sur le topic.
    for message in consumer:
        # Chaque message est déjà désérialisé grâce au paramètre value_deserializer.
        ticket = message.value
        # Affiche le ticket reçu dans la console via le logger.
        logger.info(f"🎫 Nouveau ticket reçu : {ticket}")

# Gestion de l'interruption manuelle (par exemple, via Ctrl+C) pour quitter proprement le script.
except KeyboardInterrupt:
    logger.warning("🛑 Arrêt manuel du consumer.")

# Bloc finally pour s'assurer que le consumer se ferme proprement, libérant ainsi les ressources et la connexion au broker.
finally:
    consumer.close()
    logger.info("🔌 Connexion au broker Kafka fermée proprement.")
