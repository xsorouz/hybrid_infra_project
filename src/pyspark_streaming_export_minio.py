# ============================
# ☁️ EXPORT DES TICKETS KAFKA VERS MINIO (S3-compatible)
# Ce script lit les messages d’un topic Kafka en streaming,
# les enrichit, puis les exporte toutes les 15 secondes vers MinIO au format Parquet.
# ============================

# 📦 SPARK SQL
# SparkSession est le point d'entrée principal de toutes les applications Spark
from pyspark.sql import SparkSession

# 📦 SPARK FUNCTIONS
# from_json : pour transformer une string JSON en colonne structurée
# col : permet d'accéder à une colonne
# when : pour créer une nouvelle colonne conditionnelle
from pyspark.sql.functions import from_json, col, when

# 📦 SPARK TYPES
# Utilisé pour définir la structure (schéma) des messages JSON entrants
from pyspark.sql.types import StructType, StringType

# 📦 LOGURU
# Librairie de logging avancée, utile pour suivre le pipeline étape par étape
from loguru import logger

# ============================
# 🧱 DÉFINITION DU SCHÉMA DES TICKETS
# Le message Kafka (JSON) contient ces 6 champs simples
# ============================

ticket_schema = StructType() \
    .add("ticket_id", StringType()) \
    .add("client_id", StringType()) \
    .add("created_at", StringType()) \
    .add("request", StringType()) \
    .add("request_type", StringType()) \
    .add("priority", StringType())

# ============================
# 🚀 INITIALISATION DE LA SPARKSESSION AVEC CONFIGURATION MINIO
# Configuration du client S3A pour écrire dans MinIO (via endpoint local)
# ============================

spark = SparkSession.builder \
    .appName("KafkaToMinIO") \
    .master("local[*]") \
    # 🔐 Clés d’accès MinIO (équivalentes à AWS_ACCESS_KEY_ID et SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    # 🌐 Endpoint MinIO (le service dans le réseau Docker)
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    # 🧱 Implémentation S3A standard Hadoop
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    # 🔧 Nécessaire pour que MinIO accepte les chemins comme s3a://bucket/fichier
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    # 📦 Ajout de la dépendance Hadoop AWS
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .getOrCreate()

logger.info("✅ SparkSession initialisée avec configuration S3A pour MinIO.")

# ============================
# 🔌 LECTURE DU TOPIC KAFKA (Redpanda)
# Récupère les messages du topic client_tickets
# ============================

df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \  # Adresse du broker Kafka
    .option("subscribe", "client_tickets") \               # Nom du topic
    .option("startingOffsets", "latest") \                 # On ne lit que les messages récents
    .load()

logger.info("📡 Connexion établie avec le topic Kafka 'client_tickets'.")

# ============================
# 🧪 PARSING JSON + ENRICHISSEMENT
# Transformation du champ 'value' en colonnes structurées
# ============================

df_parsed = df_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), ticket_schema).alias("data")) \
    .select("data.*")

# Ajout d'une colonne `assigned_team` selon le type de requête
df_enriched = df_parsed.withColumn(
    "assigned_team",
    when(col("request_type") == "support", "Equipe Support")
    .when(col("request_type") == "facturation", "Equipe Facturation")
    .when(col("request_type") == "technique", "Equipe Technique")
    .when(col("request_type") == "commercial", "Equipe Commerciale")
    .otherwise("Equipe Générique")
)

logger.info("🧠 Enrichissement terminé : colonne 'assigned_team' ajoutée.")

# ============================
# 📤 EXPORT VERS MINIO EN FORMAT PARQUET
# Chaque micro-batch est écrit toutes les 15 secondes dans le bucket S3
# ============================

df_enriched.writeStream \
    .format("parquet") \  # Format d’export optimisé pour analyse
    .outputMode("append") \  # Ajout des nouvelles données
    .option("path", "s3a://tickets-export/streaming_parquet") \  # Bucket et chemin MinIO
    .option("checkpointLocation", "/tmp/spark_checkpoint_minio") \  # Pour tolérance aux pannes
    .trigger(processingTime="15 seconds") \  # Déclenchement toutes les 15s
    .start() \
    .awaitTermination()  # Maintient le streaming actif

logger.success("🚀 Export vers MinIO en format Parquet lancé (toutes les 15 secondes).")
