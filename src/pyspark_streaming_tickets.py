# ============================
# 📤 EXPORT DES TICKETS KAFKA VERS FICHIERS LOCAUX (PARQUET + JSON)
# Ce script lit les tickets en streaming depuis Kafka (Redpanda),
# les enrichit et les enregistre localement en Parquet et JSON.
# ============================

# 📦 SPARK SQL
# SparkSession : point d'entrée de l'application Spark
from pyspark.sql import SparkSession

# 📦 SPARK FUNCTIONS
# from_json : transforme un champ string JSON en struct
# col : accède à une colonne
# when : construit une colonne conditionnelle (équivalent à CASE WHEN)
from pyspark.sql.functions import from_json, col, when

# 📦 SPARK TYPES
# StructType et StringType : permettent de définir le schéma des messages JSON
from pyspark.sql.types import StructType, StringType

# 📦 LOGURU
# Logger moderne et coloré pour suivre le processus
from loguru import logger

# ============================
# 🧱 DÉFINITION DU SCHÉMA DES MESSAGES KAFKA
# Le message JSON de chaque ticket contient 6 champs de type string
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
    .appName("KafkaTicketExporter") \  # Nom visible dans Spark UI
    .master("local[*]") \              # Utilisation des cœurs locaux
    .config("spark.sql.shuffle.partitions", "1") \  # Optimisation pour petit volume
    .getOrCreate()

logger.info("✅ SparkSession initialisée.")

# ============================
# 🔌 LECTURE DU FLUX DE MESSAGES KAFKA (Redpanda)
# ============================

df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \  # Adresse du broker
    .option("subscribe", "client_tickets") \                # Nom du topic
    .option("startingOffsets", "latest") \                  # Commence à lire les nouveaux messages
    .load()

logger.info("📡 Connexion au topic Kafka 'client_tickets' établie.")

# ============================
# 🧪 PARSING + ENRICHISSEMENT
# - Désérialisation JSON
# - Attribution d'une équipe selon le type de requête
# ============================

df_parsed = df_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), ticket_schema).alias("data")) \
    .select("data.*")

df_enriched = df_parsed.withColumn(
    "assigned_team",
    when(col("request_type") == "support", "Equipe Support")
    .when(col("request_type") == "facturation", "Equipe Facturation")
    .when(col("request_type") == "technique", "Equipe Technique")
    .when(col("request_type") == "commercial", "Equipe Commerciale")
    .otherwise("Equipe Générique")
)

logger.info("🧠 Colonne 'assigned_team' ajoutée aux données.")

# ============================
# 📤 EXPORT EN PARQUET (local)
# ============================

df_enriched.writeStream \
    .format("parquet") \  # Format colonne optimisé pour analyse
    .outputMode("append") \  # Ajout des nouvelles lignes au fur et à mesure
    .option("path", "./outputs/output_parquet") \  # Dossier de sortie
    .option("checkpointLocation", "./outputs/chk_parquet") \  # Pour reprise en cas d’erreur
    .trigger(processingTime="10 seconds") \  # Export toutes les 10s
    .start()

logger.success("📁 Export Parquet activé dans ./outputs/output_parquet")

# ============================
# 📤 EXPORT EN JSON (local)
# ============================

df_enriched.writeStream \
    .format("json") \  # Format JSON ligne par ligne (compatible Mongo, S3, etc.)
    .outputMode("append") \
    .option("path", "./outputs/output_json") \
    .option("checkpointLocation", "./outputs/chk_json") \
    .trigger(processingTime="10 seconds") \
    .start() \
    .awaitTermination()  # Laisse tourner le streaming en continu

logger.success("📁 Export JSON activé dans ./outputs/output_json")
