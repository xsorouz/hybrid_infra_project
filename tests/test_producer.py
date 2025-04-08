# ============================
# ✅ TEST DU PRODUCTEUR DE TICKETS
#
# Ce script de test utilise pytest pour vérifier que :
# 1. La fonction generate_ticket() retourne bien un ticket structuré
#    sous forme de dictionnaire avec tous les champs attendus.
# 2. Les valeurs générées pour "request_type" et "priority" sont valides.
# 3. (Test optionnel) Le ticket peut être envoyé à un broker Kafka (Redpanda)
#    dans le bon topic. Ce test dépend d'un Redpanda actif et est donc marqué comme skip par défaut.
# ============================

import pytest      # Framework de test pour exécuter les tests unitaires
import json        # Pour la sérialisation/désérialisation JSON
from src.producer_tickets import generate_ticket, REQUEST_TYPES, PRIORITIES  # Importation des fonctions et constantes à tester

# ============================
# 🧪 TEST UNITAIRE : STRUCTURE DU TICKET
#
# Ce test vérifie que la fonction generate_ticket() retourne un ticket :
# - Sous forme d'un dictionnaire.
# - Contenant tous les champs obligatoires.
# - Avec des valeurs valides pour "request_type" et "priority".
# ============================

def test_generate_ticket_structure():
    # Génère un ticket fictif en appelant la fonction generate_ticket()
    ticket = generate_ticket()
    
    # Vérifie que le ticket est bien un dictionnaire
    assert isinstance(ticket, dict), "Le ticket doit être un dictionnaire."
    
    # Définition des clés attendues dans le ticket
    expected_keys = {"ticket_id", "client_id", "created_at", "request", "request_type", "priority"}
    # Vérifie que toutes les clés attendues sont présentes dans le ticket généré
    assert expected_keys.issubset(ticket.keys()), "Le ticket ne contient pas tous les champs attendus."
    
    # Vérifie que la valeur du champ "request_type" est l'une des valeurs définies dans REQUEST_TYPES
    assert ticket["request_type"] in REQUEST_TYPES, "Type de requête non valide"
    # Vérifie que la valeur du champ "priority" est l'une des valeurs définies dans PRIORITIES
    assert ticket["priority"] in PRIORITIES, "Niveau de priorité non valide"

# ============================
# 🧪 TEST OPTIONNEL (à activer si Redpanda tourne) : ENVOI KAFKA
#
# Ce test vérifie que le ticket généré peut être envoyé vers un broker Kafka
# dans le topic "client_tickets". Il est marqué comme skip car il dépend d'un Redpanda actif.
# ============================

@pytest.mark.skip(reason="⚠️ Test dépendant d'un Redpanda actif — exécuter uniquement en local avec le broker lancé.")
def test_produce_ticket_to_kafka():
    # Importation locale de KafkaProducer pour éviter de charger ce test lorsque ce n'est pas nécessaire
    from kafka import KafkaProducer
    
    # Création d'un KafkaProducer configuré pour sérialiser les messages en JSON
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",  # Adresse du broker Kafka (à adapter selon l'environnement)
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    
    # Génère un ticket fictif à envoyer
    ticket = generate_ticket()
    
    # Envoie le ticket sur le topic "client_tickets"
    result = producer.send("client_tickets", value=ticket)
    
    # Récupère les métadonnées du message envoyé, avec un timeout de 10 secondes
    metadata = result.get(timeout=10)
    
    # Vérifie que le ticket a été envoyé dans le topic "client_tickets"
    assert metadata.topic == "client_tickets", "Le message n’a pas été envoyé dans le bon topic"
