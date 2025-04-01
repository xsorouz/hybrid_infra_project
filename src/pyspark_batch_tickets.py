# ============================
# 📥 LECTURE BATCH DES TICKETS VIA KAFKA (Redpanda)
# Ce script lit une fois les messages Kafka disponibles,
# les transforme, les enrichit et les affiche.
# Idéal pour des tests ou extractions ponctuelles.
# ============================

# 📦 SPARK SQL
# SparkSession permet de lancer une session Spark locale
from pyspark.sql import SparkSession

# 📦 SPARK FUNCTIONS
# from_json : convertit une string JSON en struct
# col : sélectionne une colonne dans le DataFrame
# when : création conditionnelle de colonnes
from pyspark.sql.functions import from_json, col, when

# 📦 SPARK TYPES
# StructType / StringType : schéma explicite des données attendues
from pyspark.sql.types import StructType, StringType

# 📦 LOGURU
# Pour des logs lisibles, colorés et enregistrables
from loguru import logger

# ============================
# 🧱 DÉFINITION DU SCHÉMA DES MESSAGES JSON
# Utilisé pour parser le contenu JSON du champ `value` Kafka
# ============================

ticket_schema = StructType() \
    .add("ticket_id", StringType()) \
    .add("client_id", StringType()) \
    .add("created_at", StringType()) \
    .add("request", StringType()) \
    .add("request_type", StringType()) \
    .add("priority", StringType())

# ============================
# 🚀 CRÉATION DE LA SPARK SESSION
# ============================

spark = SparkSession.builder \
    .appName("KafkaBatchReader") \
    .master("local[*]") \
    .getOrCreate()

logger.info("✅ SparkSession initialisée (mode batch).")

# ============================
# 🔌 LECTURE DES DONNÉES KAFKA EN MODE BATCH
# Le flux est lu une seule fois (pas de streaming ici)
# ============================

df_stream = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "client_tickets") \
    .option("startingOffsets", "earliest") \  # On veut récupérer tous les messages
    .option("endingOffsets", "latest") \      # Jusqu’au dernier message
    .load()

logger.info("📦 Messages Kafka chargés en mode batch depuis le topic 'client_tickets'.")

# ============================
# 🧪 PARSING ET TRANSFORMATION
# - Conversion du champ `value` en string
# - Désérialisation JSON selon le schéma
# ============================

df_parsed = df_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), ticket_schema).alias("data")) \
    .select("data.*")

# ============================
# 🛠️ ENRICHISSEMENT DES DONNÉES
# Ajout d'une colonne `assigned_team` selon le type de requête
# ============================

df_enriched = df_parsed.withColumn(
    "assigned_team",
    when(col("request_type") == "support", "Equipe Support")
    .when(col("request_type") == "facturation", "Equipe Facturation")
    .when(col("request_type") == "technique", "Equipe Technique")
    .when(col("request_type") == "commercial", "Equipe Commerciale")
    .otherwise("Equipe Générique")
)

logger.info("🧠 Données enrichies avec la colonne 'assigned_team'.")

# ============================
# 📋 AFFICHAGE DES RÉSULTATS
# ============================

df_enriched.show(truncate=False, n=20)  # Affiche les 20 premiers tickets

logger.success("📊 Lecture batch terminée avec succès.")
