# ==============================================================================
# 📊 ANALYSE DES TICKETS EXPORTÉS PAR SPARK
#
# Ce script Python a pour objectif d’analyser les tickets générés par un pipeline Spark.
# Il effectue les étapes suivantes :
#
# 1. Vérifie la présence de fichiers exportés au format Parquet ou JSON.
# 2. Charge ces fichiers dans un DataFrame Pandas.
# 3. Valide et nettoie les données (colonnes requises, doublons, valeurs manquantes).
# 4. Réalise une analyse statistique simple.
# 5. Génère plusieurs graphiques de visualisation et les sauvegarde.
# 6. Exporte un rapport CSV des données nettoyées et un résumé statistique au format texte.
# ==============================================================================

# ==============================================================================
# 📦 IMPORTATION DES LIBRAIRIES
# ==============================================================================
import pandas as pd                      # Pour la manipulation et l'analyse des données tabulaires
import matplotlib.pyplot as plt          # Pour générer et sauvegarder des graphiques
from pathlib import Path                 # Pour la gestion des chemins de fichiers de manière portable
from loguru import logger                # Pour générer des logs clairs et structurés (facilitant le débogage)

# ==============================================================================
# 📁 CONFIGURATION DES RÉPERTOIRES D'EXPORT
# ==============================================================================
# Définition du répertoire principal contenant les résultats exportés par Spark
output_base_dir = Path("data/outputs")

# Dictionnaire associant chaque format de fichier à son répertoire d'export
export_dirs = {
    "parquet": output_base_dir / "streaming_parquet",  # Répertoire pour les fichiers exportés au format Parquet
    "json": output_base_dir / "streaming_json"            # Répertoire pour les fichiers exportés au format JSON
}

# Création des répertoires d'export si ceux-ci n'existent pas déjà
# Cela permet d'éviter des erreurs lors du chargement des fichiers en cas d'exécution initiale
for path in export_dirs.values():
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)  # Crée le dossier ainsi que ses dossiers parents si nécessaire
        logger.info("📁 Dossier créé automatiquement : {}", path)

# ==============================================================================
# 🕍 CHARGEMENT DES DONNÉES
# ==============================================================================
def load_data():
    """
    Charge les données exportées par Spark en recherchant des fichiers au format Parquet ou JSON.
    
    Returns
    -------
    df : pandas.DataFrame
        DataFrame regroupant l'ensemble des tickets chargés.
    """
    # Récupération des fichiers Parquet et JSON dans leurs répertoires respectifs
    parquet_files = list(export_dirs["parquet"].glob("*.parquet"))
    json_files = list(export_dirs["json"].glob("*.json"))

    # Si des fichiers Parquet sont présents, on les charge en priorité
    if parquet_files:
        logger.info("📂 Chargement des fichiers Parquet depuis {}", export_dirs["parquet"])
        # Lecture de chaque fichier Parquet et concaténation en un seul DataFrame
        return pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)

    # Si aucun fichier Parquet n'est trouvé, bascule sur les fichiers JSON
    elif json_files:
        logger.warning("⚠️ Aucun fichier Parquet trouvé. Bascule sur les fichiers JSON...")
        logger.info("📂 Chargement des fichiers JSON depuis {}", export_dirs["json"])
        # Lecture des fichiers JSON (en mode ligne par ligne) et concaténation
        return pd.concat([pd.read_json(f, lines=True) for f in json_files], ignore_index=True)

    # Si aucun fichier n'est trouvé dans les deux répertoires, on lève une exception
    else:
        logger.error("❌ Aucun fichier .parquet ou .json trouvé dans les répertoires définis.")
        raise FileNotFoundError("Aucune donnée à analyser.")

# ==============================================================================
# 🪜 NETTOYAGE ET VALIDATION DES DONNÉES
# ==============================================================================
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Valide et nettoie le DataFrame en vérifiant la présence des colonnes requises,
    en supprimant les doublons et en éliminant les lignes contenant des valeurs manquantes.
    
    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame initial chargé contenant les données brutes des tickets.
    
    Returns
    -------
    df : pandas.DataFrame
        DataFrame nettoyé, prêt pour l'analyse statistique.
    """
    # Définition des colonnes obligatoires à retrouver dans le DataFrame
    required = ["ticket_id", "client_id", "created_at", "request_type", "priority", "assigned_team"]
    # Identification des colonnes manquantes, le cas échéant
    missing = [col for col in required if col not in df.columns]

    if missing:
        # Levée d'une erreur explicite si des colonnes essentielles sont absentes
        raise ValueError(f"Colonnes manquantes : {missing}")

    # Log de la taille initiale du DataFrame (nombre de lignes)
    logger.info("🔍 Taille initiale : {} lignes", len(df))
    # Suppression des doublons afin d'éviter la redondance dans l'analyse
    df = df.drop_duplicates()
    logger.info("🚼 Doublons supprimés. Taille : {} lignes", len(df))

    # Calcul du nombre de valeurs manquantes par colonne
    nulls = df.isna().sum()
    if nulls.any():
        # Log détaillé des colonnes comportant des valeurs manquantes
        logger.warning("⚠️ Valeurs manquantes :\n{}", nulls[nulls > 0])
        # Suppression des lignes incomplètes pour garantir l'intégrité des données
        df = df.dropna()
        logger.info("✅ Lignes incomplètes supprimées. Taille finale : {}", len(df))

    # Retourne le DataFrame nettoyé
    return df

# ==============================================================================
# 📊 ANALYSE DES DONNÉES DE TICKETS
# ==============================================================================
def analyse_tickets(df):
    """
    Effectue une analyse statistique et graphique des tickets présents dans le DataFrame.
    Cette fonction génère des graphiques, les sauvegarde et log les principales statistiques.
    
    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame pré-traité et nettoyé contenant les tickets.
    """
    # Affiche un aperçu des premières lignes pour vérification
    logger.info("\n📌 Aperçu :\n{}", df.head())
    # Log la taille du DataFrame sous forme (nombre de lignes, nombre de colonnes)
    logger.info("🔢 Taille : {} lignes × {} colonnes", *df.shape)
    # Log la liste des colonnes présentes dans le DataFrame
    logger.info("📓 Colonnes : {}", list(df.columns))

    # Si le DataFrame est vide, on log un avertissement et on arrête l'analyse
    if df.empty:
        logger.warning("⚠️ DataFrame vide. Aucune analyse possible.")
        return

    # Affiche et log les statistiques de base sur les priorités, types de requêtes et équipes assignées
    logger.info("\n📊 Priorités :\n{}", df["priority"].value_counts())
    logger.info("\n📊 Types :\n{}", df["request_type"].value_counts())
    logger.info("\n📊 Équipes :\n{}", df["assigned_team"].value_counts())

    # -----------------------
    # Graphique 1 : Barplot des priorités
    # -----------------------
    df["priority"].value_counts().plot(kind="bar", title="Tickets par priorité")
    plt.xlabel("Priorité")
    plt.ylabel("Nombre")
    plt.tight_layout()  # Ajuste la disposition pour éviter la coupure des étiquettes
    plt.savefig(output_base_dir / "graph_tickets_par_priorite.png")  # Sauvegarde du graphique dans le répertoire d'export
    plt.close()  # Ferme le graphique pour libérer la mémoire
    logger.success("📸 Graphique 1 sauvegardé.")

    # -----------------------
    # Graphique 2 : Barplot croisé des priorités par équipe
    # -----------------------
    pivot = df.pivot_table(index="assigned_team", columns="priority", aggfunc="size", fill_value=0)
    pivot.plot(kind="bar", title="Priorités par équipe")
    plt.xlabel("Équipe")
    plt.ylabel("Nombre")
    plt.tight_layout()
    plt.savefig(output_base_dir / "graph_priorite_par_equipe.png")
    plt.close()
    logger.success("📸 Graphique 2 sauvegardé.")

    # -----------------------
    # Graphique 3 : Evolution temporelle des tickets
    # -----------------------
    try:
        # Conversion de la colonne 'created_at' en format datetime pour une analyse temporelle
        df["created_at"] = pd.to_datetime(df["created_at"])
        # Comptage du nombre de tickets par date et tracé d'une courbe chronologique
        df["created_at"].dt.date.value_counts().sort_index().plot(kind="line", title="Tickets par jour")
        plt.xlabel("Date")
        plt.ylabel("Nombre")
        plt.tight_layout()
        plt.savefig(output_base_dir / "graph_tickets_par_date.png")
        plt.close()
        logger.success("📈 Graphique 3 sauvegardé.")
    except Exception as e:
        # En cas d'erreur (par exemple, lors de la conversion de la date), log l'erreur rencontrée
        logger.warning("⌛ Erreur conversion date : {}", e)

# ==============================================================================
# 🚀 POINT D'ENTRÉE DU SCRIPT
# ==============================================================================
if __name__ == "__main__":
    # Chargement des données exportées par Spark (recherche de fichiers Parquet ou JSON)
    df = load_data()

    # Nettoyage et validation des données : vérification des colonnes, suppression des doublons et des lignes incomplètes
    df = clean_data(df)

    # Réalisation de l'analyse statistique et génération des graphiques
    analyse_tickets(df)

    # -----------------------
    # Export des données nettoyées en CSV
    # -----------------------
    export_csv = output_base_dir / "tickets_analyse_export.csv"
    df.to_csv(export_csv, index=False)
    logger.success("✅ Export CSV : {}", export_csv)

    # -----------------------
    # Création et export d'un résumé texte des statistiques obtenues
    # -----------------------
    resume_txt = output_base_dir / "resume_analyse.txt"
    with open(resume_txt, "w", encoding="utf-8") as f:
        f.write(f"Répartition priorités :\n{df['priority'].value_counts()}\n\n")
        f.write(f"Types de requêtes :\n{df['request_type'].value_counts()}\n\n")
        f.write(f"Équipes assignées :\n{df['assigned_team'].value_counts()}\n")
    logger.success("🖋️ Résumé sauvegardé : {}", resume_txt)
