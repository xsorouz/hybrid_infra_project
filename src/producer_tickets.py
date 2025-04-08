# ==============================================================================
# 🚀 PRODUCTEUR DE TICKETS POUR REDPANDA (Kafka-compatible)
#
# Ce script génère des tickets fictifs à l'aide de Faker,
# puis les envoie dans un topic Kafka via Redpanda.
#
# Il est utile pour tester des pipelines Kafka,
# simuler l'arrivée de données en temps réel,
# ou encore démontrer le fonctionnement d'un producteur Kafka.
# ==============================================================================

# ==============================================================================
# 📦 IMPORTATION DES LIBRAIRIES STANDARD
# ==============================================================================
import json        # Pour convertir des objets Python en chaînes JSON
import time        # Pour gérer les temporisations, par exemple, une pause entre l'envoi des tickets
import uuid        # Pour générer des identifiants uniques (UUID) pour chaque ticket
import random      # Pour sélectionner aléatoirement des valeurs dans des listes prédéfinies
import argparse    # Pour parser les arguments passés en ligne de commande et personnaliser l'exécution du script
import os          # Pour accéder aux variables d’environnement (.env)

# ==============================================================================
# 📦 IMPORTATION DES LIBRAIRIES EXTERNES
# ==============================================================================
from faker import Faker                           # Pour générer des données factices réalistes (noms, dates, phrases, etc.)
from loguru import logger                         # Pour afficher des logs clairs et stylisés pendant l'exécution du script
from kafka import KafkaProducer, KafkaConsumer    # Pour produire et consommer des messages dans Kafka
from kafka.errors import NoBrokersAvailable       # Pour gérer les erreurs si le broker Kafka n'est pas disponible
from dotenv import load_dotenv                    # Pour charger les variables d’environnement depuis un fichier .env

# ==============================================================================
# 🔐 CHARGEMENT DES VARIABLES D’ENVIRONNEMENT
# ==============================================================================
# Charge les variables d’environnement définies dans le fichier .env à la racine du projet
load_dotenv()

# Récupère l'adresse du broker Kafka (ici Redpanda) depuis les variables d’environnement.
# Si la variable n'est pas définie, la valeur par défaut "redpanda:9092" est utilisée.
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")

# ==============================================================================
# 📌 PARAMÈTRES FIXES ET INITIALISATIONS
# ==============================================================================
# Définition de listes prédéfinies pour simuler différents types de requêtes et niveaux de priorité.
REQUEST_TYPES = ['support', 'facturation', 'technique', 'commercial']  # Types de requêtes possibles
PRIORITIES = ['low', 'medium', 'high', 'urgent']                        # Niveaux de priorité possibles

# Initialisation de Faker pour générer des données factices
fake = Faker()

# ==============================================================================
# 🧪 GÉNÉRATION D'UN TICKET ALÉATOIRE
# ==============================================================================
def generate_ticket():
    """
    Génère un ticket fictif sous forme de dictionnaire.
    
    Le ticket comprend :
    - Un identifiant unique (ticket_id) généré par uuid4.
    - Un identifiant client au format 'C' suivi de 5 chiffres, généré par Faker.
    - La date et l'heure de création au format ISO8601.
    - Une description de la requête sous forme d'une phrase.
    - Un type de requête choisi aléatoirement parmi REQUEST_TYPES.
    - Une priorité choisie aléatoirement parmi PRIORITIES.
    
    Returns
    -------
    dict
        Un ticket sous forme de dictionnaire contenant toutes les informations générées.
    """
    return {
        "ticket_id": str(uuid.uuid4()),          # Génère un identifiant unique pour le ticket
        "client_id": fake.bothify(text='C#####'),  # Génère un identifiant client, par exemple "C12345"
        "created_at": fake.iso8601(),              # Génère la date et l'heure de création en format ISO8601
        "request": fake.sentence(nb_words=6),      # Génère une phrase descriptive pour la requête
        "request_type": random.choice(REQUEST_TYPES),  # Choisit aléatoirement un type de requête
        "priority": random.choice(PRIORITIES)          # Choisit aléatoirement un niveau de priorité
    }

# ==============================================================================
# 📡 VÉRIFICATION DE L'EXISTENCE DU TOPIC KAFKA
# ==============================================================================
def topic_exists(broker, topic):
    """
    Vérifie si un topic existe dans Kafka.
    
    Cette fonction crée un KafkaConsumer pour lister les topics disponibles,
    puis effectue plusieurs tentatives (10 au total) pour vérifier l'existence du topic.
    
    Parameters
    ----------
    broker : str
        L'adresse du broker Kafka.
    topic : str
        Le nom du topic à vérifier.
    
    Returns
    -------
    bool
        True si le topic existe, False sinon.
    """
    # Création d'un KafkaConsumer pour lister les topics disponibles
    consumer = KafkaConsumer(bootstrap_servers=broker)
    for attempt in range(10):
        # Vérifie si le topic figure parmi les topics disponibles
        if topic in consumer.topics():
            logger.success(f"🎯 Topic Kafka '{topic}' détecté.")
            consumer.close()
            return True
        logger.warning(f"⏳ Topic Kafka '{topic}' introuvable... tentative {attempt + 1}/10.")
        time.sleep(3)  # Pause de 3 secondes entre les tentatives
    consumer.close()
    return False

# ==============================================================================
# 🚀 ENVOI DES TICKETS À KAFKA (PRODUCTEUR)
# ==============================================================================
def produce_tickets(producer, topic, total=None, interval=1.0):
    """
    Envoie des tickets générés aléatoirement vers un topic Kafka.
    
    Cette fonction génère un ticket, l'envoie au topic spécifié,
    et répète l'opération à un intervalle défini (en secondes).
    
    Parameters
    ----------
    producer : KafkaProducer
        Instance du producteur Kafka utilisée pour envoyer les messages.
    topic : str
        Nom du topic Kafka cible.
    total : int, optional
        Nombre total de tickets à envoyer (envoi continu si None).
    interval : float, optional
        Intervalle (en secondes) entre l'envoi de deux tickets.
    """
    count = 0  # Compteur de tickets envoyés
    logger.info(f"📤 Début de l’envoi vers le topic Kafka '{topic}'... (Appuyez sur Ctrl+C pour arrêter)")

    try:
        # Boucle infinie ou jusqu'à l'envoi du nombre total de tickets si spécifié
        while True:
            ticket = generate_ticket()             # Génère un ticket aléatoire
            producer.send(topic, value=ticket)       # Envoie le ticket dans le topic Kafka spécifié
            logger.success("✅ Ticket envoyé : {}", ticket)
            count += 1
            # Si un nombre total de tickets est défini, arrête l'envoi après avoir atteint ce nombre
            if total and count >= total:
                break
            time.sleep(interval)  # Pause entre chaque envoi pour simuler une arrivée en temps réel
    except KeyboardInterrupt:
        # Permet à l'utilisateur d'arrêter manuellement le script via Ctrl+C
        logger.warning("🛑 Arrêt manuel (Ctrl+C).")
    finally:
        # S'assure que tous les messages en attente sont envoyés avant de terminer
        producer.flush()

# ==============================================================================
# 🚦 POINT D’ENTRÉE PRINCIPAL DU SCRIPT
# ==============================================================================
if __name__ == "__main__":
    # -------------------------------
    # ▶️ Parsing des arguments en ligne de commande
    # -------------------------------
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="client_tickets", help="Nom du topic Kafka cible")
    parser.add_argument("--count", type=int, default=None, help="Nombre de tickets à envoyer (infini si omis)")
    parser.add_argument("--interval", type=float, default=1.0, help="Intervalle entre les envois (secondes)")
    args = parser.parse_args()

    # -------------------------------
    # 🔁 Connexion au broker Kafka avec tentatives de reconnexion
    # -------------------------------
    producer = None
    for attempt in range(20):
        try:
            # Création d'un KafkaProducer avec un serializer qui convertit le dictionnaire en JSON
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.success("✅ Connexion au broker Kafka établie.")
            break
        except NoBrokersAvailable:
            logger.warning(f"🔁 Broker Kafka non disponible, tentative {attempt + 1}/20...")
            time.sleep(3)  # Pause de 3 secondes avant de retenter la connexion
    else:
        logger.error("❌ Échec de la connexion à Kafka après 60 secondes.")
        raise NoBrokersAvailable()

    # -------------------------------
    # 🔍 Vérification de l’existence du topic
    # -------------------------------
    if not topic_exists(KAFKA_BROKER, args.topic):
        logger.error(f"❌ Le topic '{args.topic}' est introuvable après 10 tentatives. Abandon.")
        exit(1)

    # -------------------------------
    # ▶️ Envoi des tickets vers le topic Kafka
    # -------------------------------
    produce_tickets(producer, topic=args.topic, total=args.count, interval=args.interval)
