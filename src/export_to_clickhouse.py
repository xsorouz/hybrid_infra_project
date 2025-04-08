# ======================================================================
# 🗄️ EXPORT DES TICKETS VERS CLICKHOUSE
# ======================================================================
# Ce script permet d'exporter des tickets (données) depuis des fichiers Parquet
# vers une base de données ClickHouse. Il réalise les étapes suivantes :
#   - Chargement des variables d'environnement
#   - Lecture et concaténation des fichiers Parquet
#   - Nettoyage des données (suppression des doublons et valeurs manquantes)
#   - Création (si nécessaire) de la table dans ClickHouse
#   - Insertion des données dans la table ClickHouse

# ---------------------------
# IMPORTATION DES LIBRAIRIES
# ---------------------------

import pandas as pd
# pandas est utilisé pour la manipulation et l'analyse des données sous forme de DataFrame.

from clickhouse_driver import Client
# Client est la classe fournie par le driver ClickHouse pour établir une connexion
# et exécuter des requêtes SQL sur une base de données ClickHouse.

from loguru import logger
# loguru est une librairie de logging qui facilite la journalisation des événements
# et erreurs dans l'application.

from pathlib import Path
# Path est une classe du module pathlib qui simplifie la manipulation des chemins de fichiers
# et répertoires de manière multiplateforme.

from dotenv import load_dotenv
# load_dotenv permet de charger les variables d'environnement à partir d'un fichier .env,
# facilitant ainsi la configuration de l'application.

import os
# os fournit des fonctions pour interagir avec le système d'exploitation, ici pour accéder
# aux variables d'environnement.

from datetime import datetime
# datetime est utilisé pour manipuler des objets date et heure en Python.


# ========================================================================
# 🔐 CHARGEMENT DES VARIABLES D’ENVIRONNEMENT
# ========================================================================
load_dotenv()  # Charge les variables d'environnement définies dans le fichier .env

# Récupération des paramètres de connexion à ClickHouse depuis les variables d'environnement,
# avec des valeurs par défaut au cas où elles ne seraient pas définies.
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 9000))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "tickets_db")
CLICKHOUSE_TABLE = "tickets_analysis"  # Nom de la table de destination dans ClickHouse
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

# ============================================================================
# 📁 CHEMIN DES FICHIERS PARQUET EXPORTÉS PAR SPARK
# ============================================================================
# Définition du répertoire où sont stockés les fichiers Parquet exportés par Spark.
parquet_dir = Path("data/outputs/streaming_parquet")

def load_parquet_files():
    """
    Charge et concatène tous les fichiers Parquet présents dans le répertoire défini.
    
    Étapes :
      - Vérifie si le répertoire existe.
      - Récupère la liste des fichiers avec l'extension .parquet.
      - Si aucun fichier n'est trouvé, lève une exception.
      - Concatène les fichiers en un DataFrame unique.
      - Retourne le DataFrame contenant toutes les données.
    """
    # Vérification de l'existence du répertoire contenant les fichiers Parquet.
    if not parquet_dir.exists():
        logger.error("❌ Dossier introuvable : {}", parquet_dir)
        raise FileNotFoundError("Répertoire de fichiers Parquet non trouvé.")

    # Récupération de tous les fichiers .parquet dans le répertoire.
    files = list(parquet_dir.glob("*.parquet"))
    if not files:
        logger.error("❌ Aucun fichier Parquet dans {}", parquet_dir)
        raise FileNotFoundError("Aucun fichier .parquet détecté.")

    # Journalisation du nombre de fichiers trouvés.
    logger.info("📂 Chargement de {} fichiers depuis {}", len(files), parquet_dir)
    
    # Lecture de chaque fichier Parquet et concaténation dans un DataFrame unique.
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    
    # Journalisation du nombre de lignes chargées dans le DataFrame.
    logger.success("✅ Données chargées : {} lignes", len(df))
    return df

def clean_dataframe(df):
    """
    Nettoie le DataFrame en supprimant les doublons et les valeurs manquantes.
    
    Paramètres :
      - df : DataFrame à nettoyer.
    
    Retourne :
      - Le DataFrame nettoyé.
    """
    logger.info("🧼 Nettoyage des doublons et valeurs manquantes")
    
    # Suppression des lignes en double.
    df = df.drop_duplicates()
    # Suppression des lignes contenant des valeurs manquantes.
    df = df.dropna()
    return df

def export_to_clickhouse(df):
    """
    Exporte le DataFrame vers la base de données ClickHouse.
    
    Étapes :
      - Établit la connexion à ClickHouse.
      - Crée la table de destination si elle n'existe pas.
      - Convertit correctement les dates.
      - Insère les données dans la table.
    
    Paramètres :
      - df : DataFrame contenant les données à exporter.
    """
    logger.info("🔗 Connexion à ClickHouse sur {}:{}", CLICKHOUSE_HOST, CLICKHOUSE_PORT)
    
    # Création d'une instance client pour se connecter à ClickHouse.
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DB,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )

    # Exécution d'une requête SQL pour créer la table si elle n'existe pas.
    client.execute(f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_TABLE} (
            ticket_id       String,
            client_id       String,
            created_at      String,
            request         String,
            request_type    String,
            priority        String,
            assigned_team   String
        ) ENGINE = MergeTree()
        ORDER BY created_at
    """)
    logger.info("🛠️ Table '{}' prête dans la base '{}'", CLICKHOUSE_TABLE, CLICKHOUSE_DB)

    data = list(df.to_records(index=False))
    
    # Exécution de l'insertion des données dans la table ClickHouse.
    client.execute(f"INSERT INTO {CLICKHOUSE_TABLE} VALUES", data)
    logger.success("✅ {} lignes insérées dans '{}'", len(df), CLICKHOUSE_TABLE)

# ============================================================================
# ▶️ POINT D’ENTRÉE
# ============================================================================
if __name__ == "__main__":
    # Charge les fichiers Parquet depuis le répertoire spécifié.
    df = load_parquet_files()
    # Nettoie le DataFrame en supprimant doublons et valeurs manquantes.
    df = clean_dataframe(df)
    # Exporte les données nettoyées dans la base ClickHouse.
    export_to_clickhouse(df)
