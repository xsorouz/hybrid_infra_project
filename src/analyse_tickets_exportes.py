# ============================
# 📊 ANALYSE DES TICKETS EXPORTÉS PAR SPARK
# Ce script charge les fichiers exportés (Parquet ou JSON),
# effectue une analyse descriptive simple et génère un graphique PNG.
# ============================

# 📦 PANDAS
# Utilisé pour charger et manipuler les données sous forme de DataFrame
import pandas as pd

# 📦 MATPLOTLIB
# Utilisé pour générer des graphiques (ici : bar chart des priorités)
import matplotlib.pyplot as plt

# 📦 PATHLIB
# Permet de gérer les chemins de fichiers de manière propre et multiplateforme
from pathlib import Path

# 📦 LOGURU
# Librairie de logging moderne pour afficher les étapes et résultats proprement
from loguru import logger

# ============================
# 📁 DÉFINITION DES RÉPERTOIRES D’EXPORT À ANALYSER
# Ces dossiers doivent contenir les fichiers produits par Spark (en local)
# ============================

export_dirs = {
    "parquet": Path("outputs/output_parquet"),  # Dossier contenant les fichiers .parquet
    "json": Path("outputs/output_json")         # Dossier contenant les fichiers .json (lignes séparées)
}

# ============================
# 📄 CHARGEMENT DES DONNÉES EXPORTÉES
# Fonction qui cherche les fichiers exportés et les concatène en un seul DataFrame
# ============================
def load_data():
    if export_dirs["parquet"].exists():
        logger.info("🔄 Chargement des fichiers Parquet...")
        return pd.concat(
            [pd.read_parquet(f) for f in export_dirs["parquet"].glob("*.parquet")],
            ignore_index=True
        )
    elif export_dirs["json"].exists():
        logger.info("🔄 Chargement des fichiers JSON...")
        return pd.concat(
            [pd.read_json(f, lines=True) for f in export_dirs["json"].glob("*.json")],
            ignore_index=True
        )
    else:
        logger.error("❌ Aucun fichier exporté trouvé dans les répertoires définis.")
        raise FileNotFoundError("Aucun fichier exporté trouvé.")

# ============================
# 📊 ANALYSE DES DONNÉES
# Affiche quelques statistiques descriptives + génère un graphique
# ============================
def analyse_tickets(df):
    # Aperçu des données
    logger.info("📌 Aperçu des données :\n{}", df.head())

    # Comptage par niveau de priorité
    logger.info("📊 Nombre de tickets par priorité :\n{}", df['priority'].value_counts())

    # Comptage par type de requête
    logger.info("📊 Nombre de tickets par type de requête :\n{}", df['request_type'].value_counts())

    # Comptage par équipe assignée (créée en streaming via Spark)
    logger.info("📊 Nombre de tickets par équipe assignée :\n{}", df['assigned_team'].value_counts())

    # Génération du graphique
    df['priority'].value_counts().plot(kind='bar', title='Tickets par priorité')
    plt.tight_layout()
    plt.savefig("outputs/graph_tickets_par_priorite.png")  # Sauvegarde du graphique
    logger.success("📸 Graphique enregistré dans outputs/graph_tickets_par_priorite.png")

# ============================
# 🚀 POINT D’ENTRÉE PRINCIPAL
# ============================
if __name__ == "__main__":
    df = load_data()                                   # Chargement des fichiers exportés
    analyse_tickets(df)                                # Analyse simple
    df.to_csv("outputs/tickets_analyse_export.csv", index=False)  # Sauvegarde au format CSV
    logger.success("✅ Données analysées exportées dans outputs/tickets_analyse_export.csv")
