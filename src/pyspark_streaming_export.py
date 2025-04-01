# ============================
# 🖥️ AFFICHAGE DES TICKETS EN STREAMING DANS LA CONSOLE
# Ce script lit en continu les messages Kafka (Redpanda),
# enrichit les tickets avec une colonne `assigned_team`,
# puis les affiche en direct dans la console.
# ============================

# 📦 SPARK SQL
# SparkSession : point d’entrée de toute application Spark
from pyspark.sql import SparkSession

# 📦 SPARK FUNCTIONS
# from_json : désérialise une string JSON en struct
# col : accède aux colonnes
# when : logique conditionnelle (comme CASE WHEN SQL)
from pyspark.sql.functions import from_json, col, when

# 📦 SPARK TYPES
# StructType / StringType : définition explicite du schéma JSON
from pyspark.sql.types import StructType, StringType

# 📦 LOGURU
# Logger moderne et lisible pour traquer les étapes du pipeline
from loguru import logger

# ============================
# 🧱 DÉFINITION DU SCHÉMA DES TICKETS (JSON)
# Utilisé pour parser chaque message reçu depuis Kafka
# ============================

ticket_schema = StructType() \
    .add("ticket_id", StringType()) \
    .add("client_id", StringType()) \
    .add("created_at", StringType()) \
    .add("request", StringType()) \
    .add("request_type", StringType()) \
    .add("priority", StringType())

# ============================
# 🚀 INITIALISATION DE LA SPARK SESSION
# ============================

spark = SparkSession.builder \
    .appName("KafkaTicketStreaming") \
    .master("local[*]") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \  # Nécessaire en streaming
    .getOrCreate()

logger.info("✅ SparkSession initialisée en mode streaming.")

# ============================
# 🔌 CONNEXION AU TOPIC KAFKA (REDPANDA)
# ============================

df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "client_tickets") \
    .option("startingOffsets", "latest") \
    .load()

logger.info("📡 Connexion établie avec le topic Kafka 'client_tickets'.")

# ============================
# 🧪 PARSING DES MESSAGES + SÉLECTION DES COLONNES
# ============================

df_parsed = df_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), ticket_schema).alias("data")) \
    .select("data.*")

logger.debug("📦 Données JSON converties et structurées.")

# ============================
# 🛠️ ENRICHISSEMENT : AJOUT DE 'assigned_team'
# ============================

df_enriched = df_parsed.withColumn(
    "assigned_team",
    when(col("request_type") == "support", "Equipe Support")
    .when(col("request_type") == "facturation", "Equipe Facturation")
    .when(col("request_type") == "technique", "Equipe Technique")
    .when(col("request_type") == "commercial", "Equipe Commerciale")
    .otherwise("Equipe Générique")
)

logger.info("🧠 Colonne 'assigned_team' ajoutée dynamiquement.")

# ============================
# 📺 AFFICHAGE EN STREAMING DANS LA CONSOLE
# ============================

query = df_enriched.writeStream \
    .outputMode("append") \              # Mode d’affichage des nouvelles lignes
    .format("console") \                 # Affiche les résultats dans le terminal
    .option("truncate", False) \         # Ne pas tronquer les colonnes
    .start()

logger.success("🚀 Streaming lancé : les tickets seront affichés dans la console en continu.")

# ⏳ Maintient le processus actif (Ctrl+C pour arrêter)
query.awaitTermination()
