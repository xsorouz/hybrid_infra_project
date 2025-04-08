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
Parfait ! Voici une version **mise à jour et harmonisée** de ce bloc pour le nouveau `README.md`, adaptée à **ta stack finale** (sans `consumer`, sans `pyspark_batch_tickets.py`, avec **Metabase**, **PostgreSQL**, **DuckDB**, etc.).

---

## ✍️ Exercice 2 — Pipeline de gestion de tickets clients

### 🎯 Objectifs pédagogiques
- Simuler un flux d’événements (tickets clients) en **temps réel**
- Utiliser **PySpark Streaming** pour transformer et enrichir les messages Kafka
- Exporter automatiquement vers **MinIO** au format **Parquet**
- Charger les données dans **PostgreSQL** pour visualisation avec **Metabase**
- Permettre une analyse ad hoc avec **Pandas + DuckDB**
- Automatiser le tout via **Docker Compose**

---

### ✅ Étape 1 – Génération des tickets clients

**Script :** `src/producer_tickets.py`

- Utilise **Faker** pour générer des tickets contenant :
  - `ticket_id`, `client_id`, `created_at`, `request`, `request_type`, `priority`
- Envoie chaque message dans **Redpanda** (topic Kafka `client_tickets`)

```bash
python src/producer_tickets.py --count 100 --interval 0.5
```

> 🐳 **Dockerisé avec `Dockerfile.producer`** et intégré à `docker-compose.infra.yml`

---

### ✅ Étape 2 – Traitement avec PySpark (streaming)

**Script :** `src/pyspark_streaming_export_minio.py`

- Lit les messages Kafka du topic `client_tickets`
- Transforme les données et ajoute une colonne `assigned_team`
- Exporte les résultats toutes les 15 secondes :
  - localement dans `data/outputs/`
  - dans **MinIO** (bucket `tickets-export`)

```bash
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.1 src/pyspark_streaming_export_minio.py
```

---

### ✅ Étape 3 – Analyse & export des résultats

#### 📊 Analyse locale avec Pandas + DuckDB
**Script :** `src/analyse_tickets_exportes.py`
- Regroupe les données par `priority`, `request_type`, `assigned_team`
- Génère un graphique PNG et un CSV d'analyse

```bash
python src/analyse_tickets_exportes.py
```

#### 🗄️ Export structuré vers PostgreSQL
**Script :** `src/export_to_postgres.py`
- Envoie les fichiers `.parquet` dans une table PostgreSQL (`tickets_analysis`)
- Utilisé ensuite par **Metabase** pour créer des dashboards

---

### ✅ Étape 4 – Orchestration via Docker Compose

**Fichier principal :** `docker-compose.infra.yml`

Services déployés :
- `redpanda` : broker Kafka
- `minio` : stockage S3 local
- `postgres` : base relationnelle
- `metabase` : visualisation no-code
- `ticket-producer` : simulation en continu
- `spark-tickets` : traitement PySpark

```bash
docker-compose -f docker-compose.infra.yml up --build -d
```

> 🔁 Pour arrêter proprement : `./arreter_stack.bat`

---

## 🧩 Schéma d’architecture (Mermaid)

<details>
<summary>Clique ici pour afficher le diagramme</summary>

```mermaid
graph TD
  A[Producer Python] --> B[Kafka (Redpanda)]
  B --> C[PySpark Streaming]
  C --> D1[Parquet / JSON]
  D1 --> E[MinIO (S3 compatible)]
  D1 --> F[DuckDB / Pandas (analyse locale)]
  D1 --> G[PostgreSQL (export structuré)]
  G --> H[Metabase (dashboards)]
```

</details>

---

Souhaites-tu que je l’intègre directement dans le fichier `README.md` et que je le régénère ici proprement ?