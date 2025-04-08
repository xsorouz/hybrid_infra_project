# ==============================================================================
# 📁 EXPORT DES DONNÉES TICKETS VERS POSTGRESQL
#
# Ce script lit les fichiers Parquet exportés par Spark (pré-analyse),
# puis insère les tickets enrichis dans une base PostgreSQL.
#
# ✅ Objectifs :
# - Centraliser les données Spark dans une base SQL
# - Faciliter les requêtes ultérieures via Metabase ou pgAdmin
# - Archiver les tickets pour de futures analyses
# ==============================================================================

# ==============================================================================
# 📦 IMPORTATION DES LIBRAIRIES
# ==============================================================================
import pandas as pd                            # Pour manipuler et analyser les données sous forme de DataFrame
from pathlib import Path                       # Pour gérer les chemins de fichiers de manière indépendante du système d'exploitation
import os                                      # Pour accéder aux variables d'environnement et aux paramètres système
from sqlalchemy import create_engine           # Pour établir une connexion à PostgreSQL via SQLAlchemy
from loguru import logger                      # Pour générer des logs clairs, colorés et structurés
from dotenv import load_dotenv                 # Pour charger les variables d'environnement depuis un fichier .env

# ==============================================================================
# 🔐 CHARGEMENT DES VARIABLES D'ENVIRONNEMENT
# ==============================================================================
# Charge les variables d'environnement définies dans le fichier .env (à la racine du projet)
load_dotenv()

# Extraction des identifiants et informations de connexion à PostgreSQL
POSTGRES_USER = os.environ["POSTGRES_USER"]         # Nom d'utilisateur PostgreSQL
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"] # Mot de passe associé
POSTGRES_DB = os.environ["POSTGRES_DB"]             # Nom de la base de données à utiliser
POSTGRES_HOST = os.environ["POSTGRES_HOST"]         # Adresse ou nom du conteneur hébergeant PostgreSQL
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")   # Port d'accès à PostgreSQL (5432 par défaut)

# ==============================================================================
# 📂 DÉFINITION DU RÉPERTOIRE DES EXPORTS PARQUET
# ==============================================================================
# Chemin vers le dossier contenant les fichiers Parquet générés par Spark
parquet_dir = Path("data/outputs/streaming_parquet")

# ==============================================================================
# 🗐 CHARGEMENT DES DONNÉES PARQUET EN UN DATAFRAME
# ==============================================================================
def load_parquet_files():
    """
    Charge tous les fichiers .parquet présents dans le répertoire spécifié.
    
    Returns
    -------
    df : pandas.DataFrame
        Un DataFrame fusionné contenant l'ensemble des lignes de tous les fichiers Parquet.
    
    Raises
    ------
    FileNotFoundError
        Si le dossier ou aucun fichier Parquet n'est trouvé.
    """
    # Vérifie si le dossier contenant les fichiers Parquet existe
    if not parquet_dir.exists():
        logger.error("❌ Dossier inexistant : {}", parquet_dir)
        raise FileNotFoundError("Dossier Parquet introuvable.")

    # Recherche tous les fichiers .parquet dans le dossier spécifié
    parquet_files = list(parquet_dir.glob("*.parquet"))
    if not parquet_files:
        logger.error("❌ Aucun fichier .parquet trouvé dans : {}", parquet_dir)
        raise FileNotFoundError("Aucun fichier Parquet à charger.")

    # Log le nombre de fichiers trouvés
    logger.info("📂 {} fichiers trouvés dans {}", len(parquet_files), parquet_dir)
    # Concatène les fichiers Parquet en un seul DataFrame en ignorant les index individuels
    df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
    logger.success("✅ Données chargées avec succès. Dimensions : {} lignes", len(df))
    return df

# ==============================================================================
# 📥 EXPORT DES DONNÉES VERS LA BASE POSTGRESQL
# ==============================================================================
def export_to_postgres(df: pd.DataFrame, table_name: str = "tickets_analysis"):
    """
    Envoie le DataFrame fourni vers la table PostgreSQL cible.
    
    Parameters
    ----------
    df : pandas.DataFrame
        Données à insérer dans la base PostgreSQL.
    table_name : str, optionnel
        Nom de la table dans laquelle insérer les données (par défaut "tickets_analysis").
    """
    # Vérifie que le DataFrame n'est pas vide
    if df.empty:
        logger.warning("⚠️ Le DataFrame est vide. Aucune insertion effectuée.")
        return

    # Vérifie la présence d'une colonne essentielle ('ticket_id') pour garantir l'intégrité des données
    if "ticket_id" not in df.columns:
        logger.error("❌ Colonne obligatoire 'ticket_id' absente des données.")
        return

    try:
        # Construit l'URL de connexion PostgreSQL au format requis par SQLAlchemy
        db_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        # Crée le moteur de connexion à la base de données
        engine = create_engine(db_url)
        logger.info("🔗 Connexion à PostgreSQL établie.")

        # Utilise la méthode to_sql de pandas pour insérer le DataFrame dans la table PostgreSQL
        # L'option if_exists="replace" remplace la table existante, ce qui peut être adapté selon les besoins
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        logger.success("✅ Insertion terminée dans la table '{}' ({} lignes).", table_name, len(df))
    except Exception as e:
        # En cas d'erreur, log l'exception rencontrée
        logger.error("❌ Erreur lors de l'insertion PostgreSQL : {}", e)

# ==============================================================================
# 🚀 POINT D'ENTRÉE PRINCIPAL DU SCRIPT
# ==============================================================================
if __name__ == "__main__":
    # 1. Chargement local des fichiers Parquet exportés par Spark
    df = load_parquet_files()

    # 2. Exportation des données chargées vers PostgreSQL dans la table "tickets_analysis"
    export_to_postgres(df)
