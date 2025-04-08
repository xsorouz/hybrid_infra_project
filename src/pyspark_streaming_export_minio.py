# ==============================================================================
# ☁️ EXPORT DES TICKETS KAFKA VERS MINIO (S3-compatible)
#
# Ce script réalise les opérations suivantes :
# 1. Lecture en streaming des messages d’un topic Kafka (ici via Redpanda).
# 2. Parsing des messages JSON selon un schéma prédéfini.
# 3. Enrichissement des données en ajoutant une colonne "assigned_team" en fonction du type de demande.
# 4. Export continu des données vers MinIO :
#    - en format Parquet (data lake optimisé)
#    - en format JSON (brut lisible et indexable)
#
# Ce script permet de transformer en temps réel les données issues d’un flux Kafka
# et de les stocker dans un système de stockage S3-compatible (MinIO) pour une analyse ultérieure.
# ==============================================================================


# ==============================================================================
# --- IMPORTATION DES MODULES SPARK ET AUTRES LIBRAIRIES ---
# ==============================================================================

# Importation de la classe SparkSession pour initialiser la session Spark
from pyspark.sql import SparkSession

# Importation des fonctions de transformation pour traiter les données en streaming
from pyspark.sql.functions import from_json, col, when

# Importation des types de données pour définir le schéma des messages JSON
from pyspark.sql.types import StructType, StringType

# ==============================================================================
# --- IMPORTATION DES MODULES DE JOURNALISATION ET DE GESTION DE L'ENVIRONNEMENT ---
# ==============================================================================

from loguru import logger    # Pour un logging clair et structuré
from dotenv import load_dotenv  # Pour charger les variables d’environnement depuis un fichier .env
import os                     # Pour accéder aux variables d’environnement et interagir avec le système d’exploitation

import time

# ==============================================================================
# 🔐 CHARGEMENT DES VARIABLES D'ENVIRONNEMENT
#
# Charge les variables définies dans le fichier .env afin de configurer
# dynamiquement les paramètres de connexion au broker Kafka et au stockage MinIO.
# ==============================================================================

load_dotenv()  # Charge les variables depuis le fichier .env

# Paramètres de connexion à Kafka (Redpanda)
KAFKA_BROKER = os.environ["KAFKA_BROKER"]  # Exemple : "redpanda:9092"

# Paramètres de connexion à MinIO (S3-compatible)
MINIO_KEY = os.environ["MINIO_ROOT_USER"]         # Identifiant d'accès à MinIO
MINIO_SECRET = os.environ["MINIO_ROOT_PASSWORD"]    # Mot de passe pour MinIO
MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]       # URL du service MinIO

# Chemin S3 (via s3a) du bucket où seront stockés les fichiers Parquet
S3_PARQUET_PATH = os.getenv("S3_BUCKET_PATH", "s3a://tickets-export/streaming_parquet")
S3_JSON_PATH = os.getenv("S3_JSON_BUCKET_PATH", "s3a://tickets-export/streaming_json")
S3_CHECKPOINT_PATH = os.getenv("S3_CHECKPOINT_PATH", "s3a://tickets-export/checkpoints/client_tickets")
STREAM_DURATION = int(os.getenv("STREAM_DURATION_SECONDS", 60))  

# ==============================================================================
# 🧱 DÉFINITION DU SCHÉMA DES TICKETS
#
# Le schéma définit la structure des messages JSON reçus depuis Kafka.
# Chaque champ est défini comme une chaîne de caractères pour simplifier l'analyse.
# ==============================================================================

ticket_schema = (
    StructType()
    .add("ticket_id", StringType())      # Identifiant unique du ticket
    .add("client_id", StringType())      # Identifiant du client
    .add("created_at", StringType())     # Date et heure de création du ticket
    .add("request", StringType())        # Description du ticket
    .add("request_type", StringType())   # Type de requête (ex : support, facturation, etc.)
    .add("priority", StringType())       # Niveau de priorité du ticket
)

# ==============================================================================
# 🚀 INITIALISATION DE LA SPARKSESSION AVEC CONFIGURATION POUR MINIO
#
# La SparkSession est configurée pour permettre l'écriture sur un stockage S3-compatible.
# Les paramètres suivants sont définis :
# - Clé et secret pour l'accès S3
# - Endpoint du service MinIO
# - Utilisation du package Hadoop AWS pour l'intégration S3
# ==============================================================================

spark = (
    SparkSession.builder
    .appName("KafkaToMinIO")                   # Nom de l'application Spark
    .master("local[*]")                        # Utilisation de tous les cœurs disponibles en local
    .config("spark.hadoop.fs.s3a.access.key", MINIO_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")  # Mode d'accès style chemin pour MinIO
    # Ajout des packages nécessaires pour l'intégration avec Kafka et Hadoop AWS
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.1")
    .getOrCreate()  # Crée ou récupère une SparkSession existante
)

logger.info("✅ SparkSession initialisée avec configuration S3A pour MinIO.")


# ================================
# 🔎 VÉRIFICATION DE LA CONFIGURATION
# ================================

try:
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    endpoint = conf.get("fs.s3a.endpoint")
    logger.info("🔗 Endpoint MinIO configuré : {}", endpoint)

    if not endpoint:
        raise ValueError("Le endpoint MinIO n’est pas configuré correctement.")

except Exception as e:
    logger.error("❌ Échec de la configuration S3A : {}", e)
    raise


# ==============================================================================
# 🔌 LECTURE DU TOPIC KAFKA (REDPANDA)
#
# Mise en place d'une source de données en streaming depuis Kafka.
# Les messages provenant du topic "client_tickets" seront lus en continu.
# ==============================================================================

df_stream = (
    spark.readStream
    .format("kafka")  # Indique que la source est Kafka
    .option("kafka.bootstrap.servers", KAFKA_BROKER)  # Adresse du broker Kafka
    .option("subscribe", "client_tickets")  # Nom du topic à écouter
    .option("startingOffsets", "latest")  # Lire uniquement les messages à partir de maintenant
    .load()  # Charge le flux de données sous forme de DataFrame en streaming
)

logger.info("📡 Connexion établie avec le topic Kafka 'client_tickets'.")

# ==============================================================================
# 🧪 PARSING DES MESSAGES JSON ET ENRICHISSEMENT
#
# Transformation du flux brut :
# 1. Conversion du contenu binaire en chaîne de caractères.
# 2. Parsing du JSON en colonnes structurées selon le schéma défini.
# 3. Ajout d'une colonne "assigned_team" en fonction du type de demande.
# ==============================================================================

# Conversion et parsing du JSON
df_parsed = (
    df_stream.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), ticket_schema).alias("data"))
    .select("data.*")
)

# Enrichissement des données : ajout de la colonne "assigned_team"
df_enriched = df_parsed.withColumn(
    "assigned_team",
    when(col("request_type") == "support", "Equipe Support")
    .when(col("request_type") == "facturation", "Equipe Facturation")
    .when(col("request_type") == "technique", "Equipe Technique")
    .when(col("request_type") == "commercial", "Equipe Commerciale")
    .otherwise("Equipe Générique")
)

logger.info("🧠 Colonne 'assigned_team' ajoutée avec succès.")

# ==============================================================================
# 📤 EXPORT CONTINU VERS MINIO EN FORMAT PARQUET
#
# Les données enrichies sont écrites en continu dans un bucket MinIO.
# Le format Parquet est utilisé pour sa compacité et son efficacité d'interrogation.
# Le streaming est configuré pour écrire toutes les 15 secondes.
# ==============================================================================

# 🔸 Export Parquet : pour stockage optimisé
query_parquet = df_enriched.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", S3_PARQUET_PATH) \
    .option("checkpointLocation", S3_CHECKPOINT_PATH) \
    .trigger(processingTime="15 seconds") \
    .start()

# 🔸 Export JSON : pour stockage brut et indexation
query_json = df_enriched.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("path", S3_JSON_PATH) \
    .option("checkpointLocation", S3_CHECKPOINT_PATH + "_json") \
    .trigger(processingTime="15 seconds") \
    .start()

logger.success("🚀 Export vers MinIO lancé avec succès en Parquet et JSON.")
 
query_parquet.awaitTermination()
# query_json.awaitTermination()  # Si tu veux l'attendre aussi