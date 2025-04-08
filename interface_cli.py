# ===============================
# 🧑‍💻 INTERFACE CLI – DATA PIPELINE HYBRIDE
# -------------------------------
# Ce script Python permet de piloter tous les fichiers .bat du projet
# pour automatiser et sécuriser le lancement du pipeline complet.
# Il offre une interface en ligne de commande pour lancer différentes actions,
# telles que le lancement de la stack, l'initialisation de l'environnement,
# l'exécution des traitements Spark, la production de tickets Kafka, l'analyse
# des fichiers exportés, et l'export des données vers ClickHouse ou PostgreSQL.
# ===============================

# Importation des modules nécessaires
import os            # Pour interagir avec le système d'exploitation (gestion des chemins, vérification de fichiers, etc.).
import subprocess    # Pour exécuter des commandes et scripts externes (ici, les scripts batch et commandes Docker).
import sys           # Pour interagir avec l'interpréteur Python (par exemple, pour quitter le script).
from colorama import Fore, Style, init  # Pour ajouter des couleurs aux sorties console afin de mieux visualiser l'interface.

# Initialisation de colorama pour la coloration du terminal.
# L'option `autoreset=True` permet de réinitialiser automatiquement les styles après chaque impression.
init(autoreset=True)

# 🔁 Se positionner à la racine du projet
# Récupère le répertoire dans lequel se trouve ce script.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Vérifie si le dossier "bat" existe dans SCRIPT_DIR.
# Si oui, SCRIPT_DIR est considéré comme racine, sinon on remonte d'un niveau.
ROOT_DIR = SCRIPT_DIR if os.path.exists(os.path.join(SCRIPT_DIR, "bat")) else os.path.dirname(SCRIPT_DIR)
# Change le répertoire de travail courant pour être positionné à la racine du projet.
os.chdir(ROOT_DIR)

# 📁 Dossier où sont stockés tous les scripts batch (.bat)
BAT_DIR = "bat"  # Chemin relatif du répertoire contenant les fichiers batch.

# 🗂️ Liste des actions disponibles avec leurs fichiers et descriptions
# Ce dictionnaire associe à chaque option de menu une paire composée :
#   - du nom du fichier batch à exécuter,
#   - d'une description textuelle de l'action correspondante.
BATCH_FILES = {
    "01": ("00_lancer_stack.bat", "▶️ Lancer la stack complète (Docker Compose)"),
    "02": ("01_initialiser_env.bat", "🧱 Initialiser Kafka + MinIO (topic + bucket)"),
    "03": ("02_lancer_traitement_spark.bat", "⚙️ Lancer le traitement Spark (streaming vers MinIO)"),
    "04": ("03_produire_tickets.bat", "📤 Produire des tickets Kafka (ex: 30 tickets)"),
    "05": ("04_analyse_offline.bat", "📊 Analyser les fichiers exportés depuis MinIO"),
    "06": ("05_export_clickhouse.bat", "🚀 Exporter les données vers ClickHouse"),
    "07": ("06_export_postgres.bat", "🛢️ Exporter les données vers PostgreSQL"),
    "08": ("07_arreter_streaming_spark.bat", "⏹️ Arrêter Spark après 60 sec (streaming auto-stop)"),
    "09": ("08_redemarrer_streaming_spark.bat", "🔄 Redémarrer Spark (streaming relancé)"),
    "10": ("09_arreter_stack.bat", "🧯 Arrêter tous les conteneurs (stack complète)"),
    "11": ("10_reset_stack.bat", "💣 Réinitialiser entièrement le projet"),
    "12": ("", "🗑️ Nettoyer le dossier data/outputs (parquet/json)"),
    "13": ("", "📦 Vérifier si des fichiers MinIO sont présents (test S3 via mc)"),
    "14": ("", "🧭 Afficher l’état de la stack (conteneurs Docker actifs)"),
    "15": ("", "📺 Ouvrir les logs Docker (Redpanda / Spark / MinIO)"),
    "00": ("", "❌ Quitter l’interface")
}

# 🖥️ Fonction d'affichage du menu
def afficher_menu():
    """
    Affiche le menu principal de l'interface CLI.
    Parcourt le dictionnaire BATCH_FILES et affiche chaque option avec son numéro et sa description,
    en utilisant des couleurs pour une meilleure lisibilité.
    """
    # Affichage d'un en-tête coloré dans le terminal.
    print(f"\n{Fore.CYAN}{'=' * 60}")
    print(f"{Fore.GREEN}📦 INTERFACE DE CONTRÔLE - STACK HYBRIDE (Kafka / Spark / S3)")
    print(f"{Fore.CYAN}{'=' * 60}{Style.RESET_ALL}")
    # Parcours du dictionnaire pour afficher chaque option disponible.
    for key, (_, description) in BATCH_FILES.items():
        print(f"{Fore.YELLOW}{key}. {description}")
    # Affichage d'un pied de page.
    print(f"{Fore.CYAN}{'=' * 60}")

# ▶️ Fonction pour exécuter un script batch (.bat)
def executer_batch(fichier_bat):
    """
    Exécute un script batch situé dans le répertoire BAT_DIR dans une nouvelle fenêtre de commande.
    
    Paramètres :
      fichier_bat (str): Le nom du fichier batch à exécuter.
    
    Ce processus :
      - Construit le chemin absolu du fichier batch.
      - Vérifie que le fichier existe.
      - Lance le script dans une nouvelle fenêtre (cmd.exe) en se positionnant d'abord dans le dossier BAT_DIR.
    """
    # Construit le chemin relatif vers le fichier batch.
    chemin_relatif = os.path.join(BAT_DIR, fichier_bat)
    # Convertit le chemin relatif en chemin absolu.
    chemin_absolu = os.path.abspath(chemin_relatif)

    # Vérifie que le fichier batch existe.
    if not os.path.isfile(chemin_absolu):
        print(f"{Fore.RED}❌ Fichier introuvable : {chemin_absolu}")
        return

    try:
        # Récupère le chemin absolu du dossier contenant les scripts batch.
        dossier_bat = os.path.abspath(BAT_DIR)
        # Exécute la commande via cmd.exe en ouvrant une nouvelle fenêtre :
        # - 'start' ouvre une nouvelle fenêtre de commande.
        # - '/K' permet de garder la fenêtre ouverte après l'exécution.
        # - La commande change d'abord le répertoire courant vers le dossier des .bat, puis exécute le fichier.
        subprocess.Popen(
            f'start cmd.exe /C "cd /d {dossier_bat} && {fichier_bat}"',
            shell=True
        )
        print(f"{Fore.GREEN}🟢 Script lancé depuis le dossier : {dossier_bat}")
    except Exception as e:
        # En cas d'erreur, affiche le message d'erreur.
        print(f"{Fore.RED}⚠️ Erreur lors de l’exécution du script : {e}")

# 📡 Fonction pour ouvrir les logs Docker dans une nouvelle fenêtre
def ouvrir_logs():
    """
    Ouvre une nouvelle fenêtre de commande pour afficher en continu (option -f)
    les logs des services Docker (redpanda, spark-tickets, minio) à partir du fichier
    de configuration docker-compose.infra.yml.
    
    En cas d'échec, affiche un message d'erreur.
    """
    try:
        subprocess.Popen([
            "cmd.exe", "/k",
            "docker compose -f docker-compose.infra.yml logs -f redpanda spark-tickets minio"
        ])
        print("📡 Fenêtre de logs ouverte.")
    except Exception as e:
        print(f"⚠️ Impossible d'ouvrir les logs : {e}")

# Fonction pour nettoyer les dossiers de sortie des fichiers (parquet et json)
def nettoyer_outputs():
    """
    Supprime tous les fichiers présents dans les dossiers définis pour les sorties
    du pipeline (ici "data/outputs/streaming_parquet" et "data/outputs/streaming_json").
    
    Pour chaque dossier :
      - Vérifie s'il existe.
      - Liste et supprime chaque fichier présent.
      - Affiche le nombre de fichiers supprimés ou un avertissement si le dossier est introuvable.
    """
    dossiers = ["data/outputs/streaming_parquet", "data/outputs/streaming_json"]
    for dossier in dossiers:
        if os.path.exists(dossier):
            fichiers = os.listdir(dossier)
            for f in fichiers:
                os.remove(os.path.join(dossier, f))
            print(f"{Fore.GREEN}🧾 {len(fichiers)} fichiers supprimés dans : {dossier}")
        else:
            print(f"{Fore.YELLOW}⚠️ Dossier non trouvé : {dossier}")

# Fonction pour nettoyer la console
def nettoyer_console():
    """
    Efface le contenu de la console en exécutant la commande appropriée
    selon le système d'exploitation (cls pour Windows, clear pour Unix).
    """
    os.system("cls" if os.name == "nt" else "clear")

# ✅ Fonction pour vérifier si la stack Docker est en cours d’exécution
def stack_est_lancee():
    """
    Vérifie si la stack Docker est lancée en testant l'état du conteneur 'redpanda'.
    
    Utilise la commande 'docker inspect' pour interroger l'état du conteneur.
    
    Retourne :
      bool: True si le conteneur 'redpanda' est en cours d'exécution, sinon False.
    """
    try:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", "redpanda"],
            capture_output=True, text=True
        )
        # Compare la sortie avec la chaîne "true" pour déterminer si le conteneur est actif.
        return result.stdout.strip() == "true"
    except Exception:
        # En cas d'erreur (par exemple, conteneur inexistant), retourne False.
        return False

# Fonction pour vérifier la présence des fichiers MinIO à l'aide du client mc
def verifier_minio():
    """
    Vérifie si des fichiers sont présents dans le bucket MinIO en utilisant le client mc.
    
    La commande docker run exécute temporairement le container 'minio/mc'
    pour lister le contenu du bucket 'tickets-export'.
    
    En cas d'échec, un message d'erreur est affiché.
    """
    try:
        subprocess.run([
            "docker", "run", "--rm", "--network=host",
            "-e", "MC_HOST_minio_local=http://admin:admin123@localhost:9000",
            "minio/mc", "ls", "minio_local/tickets-export"
        ], check=True)
    except subprocess.CalledProcessError:
        print(f"{Fore.RED}❌ Échec lors de la vérification MinIO avec mc.")

# Fonction pour afficher l'état de la stack Docker
def afficher_status_stack():
    """
    Affiche l'état des conteneurs Docker actifs ainsi que l'état des services clés
    (redpanda, spark-tickets, minio).
    
    Exécute la commande 'docker ps' avec un formatage personnalisé pour une
    lecture claire, puis pour chaque service clé, interroge son état via 'docker inspect'.
    """
    try:
        print(f"{Fore.CYAN}🔍 État des conteneurs Docker en cours :\n")
        subprocess.run(["docker", "ps", "--format", "table {{.Names}}\t{{.Status}}\t{{.Ports}}"], check=True)

        print("\n🧠 Services clés :")
        for service in ["redpanda", "spark-tickets", "minio"]:
            result = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Running}}", service],
                capture_output=True, text=True
            )
            etat = "✅ Actif" if result.stdout.strip() == "true" else "❌ Inactif"
            print(f"   - {service:<15} {etat}")
    except Exception as e:
        print(f"{Fore.RED}❌ Impossible d'afficher l'état des conteneurs. Erreur : {e}")

# 🚦 Point d’entrée principal du script
if __name__ == "__main__":
    # Nettoyage de la console dès le lancement pour une meilleure lisibilité.
    nettoyer_console()
    # Boucle principale infinie pour afficher le menu et traiter les choix de l'utilisateur.
    while True:
        afficher_menu()  # Affiche le menu principal avec les options disponibles.
        choix = input("👉 Choix (00 à 15) : ").strip()  # Demande à l'utilisateur de saisir son choix.

        # Vérifie que le choix de l'utilisateur est valide (présent dans le dictionnaire).
        if choix not in BATCH_FILES:
            print(f"{Fore.RED}❌ Choix invalide. Réessaie.")
            continue

        # Gestion des différentes options selon le choix de l'utilisateur.
        if choix == "0":
            # Option pour quitter l'interface CLI.
            print(f"{Fore.GREEN}👋 À bientôt !")
            sys.exit(0)  # Termine l'exécution du script.
        elif choix == "15":
            # Option pour ouvrir une fenêtre de logs Docker.
            ouvrir_logs()
        elif choix == "12":
            # Option pour nettoyer les dossiers contenant les fichiers de sortie.
            nettoyer_outputs()
        elif choix == "13":
            # Option pour vérifier la présence de fichiers dans MinIO.
            verifier_minio()
        elif choix == "14":
            # Option pour afficher l'état de la stack Docker et ses services clés.
            afficher_status_stack()
        elif choix == "2" and not stack_est_lancee():
            # Cas particulier : si l'utilisateur souhaite initialiser l'environnement (option 2)
            # mais que la stack Docker n'est pas en cours d'exécution, affiche un message d'erreur.
            print("❌ Stack non lancée. Merci d’exécuter d’abord l’option 1.")
            continue
        else:
            # Pour les autres options, récupère le nom du script batch associé et l'exécute.
            fichier_bat, _ = BATCH_FILES[choix]
            if fichier_bat:
                executer_batch(fichier_bat)
            else:
                print(f"{Fore.RED}⚠️ Script non défini pour ce choix.")
