# Projet : Modélisez une infrastructure dans le cloud

Ce projet OpenClassrooms est divisé en deux exercices complémentaires visant à concevoir, modéliser et simuler une **infrastructure hybride** ainsi qu'un **pipeline temps réel de traitement de données**. Tout est réalisé **localement** avec Docker sous Windows, sans aucun coût cloud.

Le document suivant présente chaque étape comme un **cours structuré**, avec explications, commandes commentées, schémas et bonnes pratiques.

---

## ✍️ Exercice 1 – Infrastructure hybride locale simulée avec Docker

### ▶ Objectifs pédagogiques
- Comprendre les principes d'une architecture cloud hybride
- Déployer des composants de traitement et de stockage distribués
- Simuler une infrastructure cloud on-premise à l'aide de Docker
- Tester les interactions entre services via des scripts Python

### ▶ Étapes détaillées

#### 1. Choix des composants

| Composant     | Rôle                                                  | Image Docker utilisée                  |
|---------------|--------------------------------------------------------|----------------------------------------|
| Redpanda      | Streaming temps réel Kafka-compatible (IoT/logs)       | `vectorized/redpanda`                  |
| MinIO         | Stockage objet (équivalent Amazon S3)                  | `minio/minio`                          |
| ClickHouse    | Entrepôt de données analytique                         | `clickhouse/clickhouse-server`         |
| PostgreSQL    | Base SQL relationnelle (optionnelle)                   | `postgres:13`                          |
| ActiveDirectory | Gestion des identités (simulée localement)           | (optionnel, non conteneurisé)          |

Tous les services sont interconnectés via un réseau Docker : `infra_hybride_default`.

#### 2. Lancement de l'infrastructure

- Vérifiez que Docker Desktop est installé et actif sur votre poste Windows.
- Placez-vous dans le dossier contenant le fichier `docker-compose.infra.yml`.

```bash
docker-compose up -d
```

Cette commande télécharge les images nécessaires, crée les conteneurs et les démarre en arrière-plan.

#### 3. Accès aux interfaces Web

| Service     | URL / Port                     | Identifiants par défaut           |
|-------------|---------------------------------|-----------------------------------|
| MinIO       | http://localhost:9001          | admin / admin123                  |
| Redpanda    | http://localhost:9644          | N/A (API Admin seulement)         |
| ClickHouse  | http://localhost:8123          | anonyme                           |
| PostgreSQL  | localhost:5432 (via client SQL)| user / pass                       |

#### 4. Tests via scripts Python

Ces scripts vous permettent de tester le bon fonctionnement de chaque composant individuellement ou collectivement :

| Script                 | Rôle                                          |
|------------------------|-----------------------------------------------|
| `test_redpanda.py`     | Publier un message Kafka                     |
| `test_minio.py`        | Créer un bucket et y envoyer un fichier      |
| `test_clickhouse.py`   | Créer une table ClickHouse                   |
| `test_postgres.py`     | Créer une table PostgreSQL                   |
| `test_all_services.py` | Enchaîner tous les tests                     |

Avant de lancer les scripts :
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

#### 5. Dockerisation des tests

Construisez une image Docker à partir de vos scripts :
```bash
docker build -t infra-tests .
docker run --network=infra_hybride_default infra-tests
```

#### 6. Commandes Docker utiles

```bash
docker ps                         # Voir les conteneurs actifs
docker-compose down -v           # Tout arrêter et supprimer volumes
docker rmi infra-tests           # Supprimer l'image des tests
docker volume prune              # Nettoyer les volumes non utilisés
```

---

Voici la **nouvelle version complète de la partie 2 du README**, avec toutes les améliorations apportées :

---

## ✍️ Exercice 2 – Pipeline de gestion de tickets clients (Redpanda + PySpark)

### ▶ Objectifs pédagogiques
- Simuler la génération d’événements (tickets clients)
- Lire et transformer ces données avec **PySpark Streaming**
- Exporter les résultats localement **et vers MinIO**
- Automatiser le tout via **Docker Compose**
- Analyser les résultats produits avec **Pandas/Matplotlib**
- Visualiser les flux dans un **schéma d’architecture**

---

### ✅ Étape 1 – Génération des tickets clients

**Scripts :** `producer_tickets.py`, `consumer_tickets_test.py`

- Le **producteur** simule des tickets clients avec les champs :  
  `ticket_id`, `client_id`, `created_at`, `request_type`, `priority`, `request`
- Le **consumer** permet d'observer les messages Kafka en temps réel.

```bash
python producer_tickets.py --count 100 --interval 0.5
```

#### 🐳 Dockerisation :
```bash
docker-compose build ticket-producer
docker-compose up ticket-producer
```

> ✅ Le **consumer est également dockerisé** via `Dockerfile.consumer` et intégré à `docker-compose.infra.yml`.

---

### ✅ Étape 2 – Traitement avec PySpark (streaming)

**Scripts principaux :**
- `pyspark_batch_tickets.py` (lecture ponctuelle)
- `pyspark_streaming_tickets.py` (streaming + export local Parquet + JSON)
- `pyspark_streaming_export_minio.py` (streaming + export vers MinIO)

Fonctions clés :
- Lecture des messages depuis Kafka (`client_tickets`)
- Parsing JSON structuré
- Ajout dynamique d’une colonne `assigned_team` selon `request_type`

---

### ✅ Étape 3 – Export des résultats

🗂️ **Exports locaux** :
- Parquet → `./outputs/output_parquet`
- JSON → `./outputs/output_json`

☁️ **Export vers MinIO (S3-compatible)** :
- Bucket : `s3a://tickets-export/streaming_parquet`
- Déclenchement toutes les 15 secondes
- Support natif de `s3a://` dans Spark avec `hadoop-aws`

```bash
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.1 pyspark_streaming_export_minio.py
```

---

### ✅ Étape 4 – Orchestration via Docker Compose

**Fichier principal :** `docker-compose.infra.yml`

Services concernés :
- `redpanda` (Kafka broker)
- `minio` (stockage objet)
- `ticket-producer` (producteur Kafka)
- `ticket-consumer` (consumer Kafka de debug)
- `spark-tickets` (traitement PySpark streaming)

```bash
docker-compose up --build -d
docker-compose logs -f spark-tickets
```

---

### ✅ Étape 5 – Analyse des résultats

**Script :** `analyse_tickets_exportes.py`

Fonctionnalités :
- Chargement des fichiers `.json` ou `.parquet`
- Statistiques agrégées : types de requêtes, priorités, équipes assignées
- Génération d’un graphique (`graphique_priorites.png`)
- Export du résumé au format `.csv`

```bash
python analyse_tickets_exportes.py
```

---
 
## 🧩 Schéma d’architecture (Mermaid)

<details>
<summary>Clique ici pour afficher le diagramme</summary>

```mermaid
graph TD
  A[Producer Python] --> B[Kafka (Redpanda)]
  B --> C[Consumer Kafka (live)]
  B --> D[PySpark Streaming]
  D --> E[+ Colonne assigned_team]
  E --> F1[Export Parquet/JSON]
  E --> F2[Export vers MinIO]
  F1 --> G[Analyse avec Pandas]
  F2 --> G
```

</details>
```
 