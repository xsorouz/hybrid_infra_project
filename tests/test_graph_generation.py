# ============================
# 🖼️ TEST DU FICHIER GRAPHIQUE GÉNÉRÉ PAR L'ANALYSE
#
# Ce test vérifie que le fichier graphique généré par la fonction d'analyse existe
# et qu'il n'est pas vide (ce qui pourrait indiquer un problème lors de sa création).
# ============================

import os                      # Module pour interagir avec le système d'exploitation (non utilisé directement ici)
from pathlib import Path       # Permet de manipuler les chemins de fichiers de manière portable

def test_graph_file_exists_and_not_empty():
    # Définition du chemin du fichier graphique attendu.
    # Ce fichier doit avoir été généré par la fonction d'analyse.
    graph_path = Path("data/outputs/graph_tickets_par_priorite.png")
    
    # Vérifie que le fichier graphique existe bien.
    assert graph_path.exists(), "❌ Le fichier graphique n'existe pas."
    
    # Vérifie que la taille du fichier est supérieure à 0 octet, ce qui indique que le fichier contient des données.
    # Si la taille est 0, le fichier serait considéré comme vide ou corrompu.
    assert graph_path.stat().st_size > 0, "❌ Le graphique est vide ou corrompu."
