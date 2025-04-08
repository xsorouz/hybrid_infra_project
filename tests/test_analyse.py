# ============================
# ✅ TEST D'ANALYSE — Vérifie que l'analyse fonctionne sans erreur
#
# Ce script de test utilise pytest pour s'assurer que :
# 1. La fonction load_data() retourne bien un DataFrame non vide.
# 2. La fonction analyse_tickets() s'exécute sans lever d'erreurs
#    et génère correctement le graphique attendu.
# ============================

# ----------------------------
# IMPORT DES BIBLIOTHÈQUES DE TEST
# ----------------------------
import pytest                   # Framework de test pour exécuter les tests unitaires
import pandas as pd             # Pour vérifier que le résultat est bien un DataFrame
from pathlib import Path        # Pour manipuler les chemins de fichiers (vérification de l'existence du graphique)

# ----------------------------
# IMPORT DES FONCTIONS À TESTER
# ----------------------------
# On importe les fonctions load_data et analyse_tickets depuis le module d'analyse.
# Ce module se trouve dans le répertoire 'src' et porte le nom 'analyse_tickets_exportes'.
from src.analyse_tickets_exportes import load_data, analyse_tickets

# ============================
# 📁 TEST DU CHARGEMENT DES DONNÉES
# ============================

def test_load_data_returns_dataframe():
    """
    Teste que la fonction load_data() :
    - Retourne un objet de type pandas.DataFrame.
    - Le DataFrame retourné n'est pas vide.
    """
    # Appel de la fonction load_data pour récupérer les données
    df = load_data()
    # Vérifie que le résultat est bien un DataFrame
    assert isinstance(df, pd.DataFrame)
    # Vérifie que le DataFrame n'est pas vide
    assert not df.empty, "Le DataFrame ne doit pas être vide."

# ============================
# 📊 TEST D'ANALYSE DES DONNÉES
# ============================

def test_analyse_executes_without_errors(tmp_path):
    """
    Teste que la fonction analyse_tickets() s'exécute sans erreurs et
    génère un fichier graphique de répartition des tickets par priorité.
    
    Paramètre:
    - tmp_path: Fixture de pytest pour la création d'un chemin temporaire 
                (non utilisé directement ici, mais souvent utile pour des tests de fichiers).
    """
    # Chargement des données via load_data()
    df = load_data()
    # Exécution de la fonction d'analyse qui doit produire un graphique en barres
    analyse_tickets(df)

    # Définition du chemin attendu du graphique généré
    graph_path = Path("data/outputs/graph_tickets_par_priorite.png")
    # Vérifie que le fichier graphique existe bien après l'exécution de la fonction
    assert graph_path.exists(), "Le graphique n'a pas été généré."
