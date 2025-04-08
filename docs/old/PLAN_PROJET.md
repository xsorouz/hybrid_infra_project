# 🗂️ PLAN_PROJET.md — Pipeline de gestion des tickets clients

Ce fichier décrit toutes les étapes du projet de traitement de tickets clients en temps réel, de la génération à la visualisation, en utilisant une stack locale optimisée (Redpanda, Spark, MinIO, DuckDB, PostgreSQL, Metabase).

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

## 🖼️ Schéma d’architecture (version Draw.io)

📁 Tu peux [télécharger la version visuelle ici (format `.drawio` ou `.png`)](./docs/schemas/pipeline_tickets.drawio) pour l’ouvrir dans [https://draw.io](https://draw.io) ou l’intégrer dans une présentation.

![Aperçu](./docs/schemas/pipeline_tickets.png)

---

## 🧱 1. Infrastructure (Docker)

### Objectifs
- Lancer une stack locale complète
- Zéro action manuelle après `lancer_stack.bat`

### Composants
- Redpanda (Kafka-compatible)
- MinIO (stockage objet)
- PostgreSQL (base relationnelle)
- Metabase (visualisation)
- Services internes : producer, spark

### Lancement
```bash
./lancer_stack.bat
```

📌 Toutes les variables sensibles (identifiants, mots de passe) sont externalisées dans le fichier `.env` à la racine du projet.

Une fois terminé :
```bash
./arreter_stack.bat
```
Cela arrête tous les services et nettoie `./data/`.

---

## 🎲 2. Génération des données (Producer Kafka)

### Script : `src/producer_tickets.py`
- Génère des "tickets clients" aléatoires via Faker
- Envoie les messages dans Redpanda (topic Kafka)

### Objectifs
- Simuler des événements clients (type, priorité, date…)

---

## 🔥 3. Traitement Spark (Streaming)

### Script : `src/pyspark_streaming_export_minio.py`
- Lit les messages depuis Redpanda
- Ajoute une colonne `assigned_team` selon la logique métier
- Exporte les résultats au format Parquet et/ou JSON
  - Dans `data/outputs/`
  - Dans MinIO (via `s3a://`)

### Format cible : `.parquet` (Delta non utilisé ici)

---

## 📁 4. Stockage des fichiers

### Local
- Tous les fichiers produits par Spark sont placés dans :
```bash
./data/outputs/
```

### MinIO (S3 compatible)
- Les mêmes fichiers sont disponibles dans un bucket MinIO (`tickets-export`) pour consultation via API ou interface Web.

---

## 🧪 5. Analyse locale (Pandas + DuckDB)

### Script : `src/analyse_tickets_exportes.py`
- Ouvre les fichiers `.parquet` produits par Spark
- Fait des regroupements (par priorité, par équipe…)
- Sauvegarde un rapport CSV ou PNG

### Avantage : DuckDB permet des requêtes SQL locales ultra rapides sans base serveur.

---

## 🗄️ 6. Export structuré (PostgreSQL)

### Script : `src/export_to_postgres.py`
- Charge les fichiers `.parquet`
- Envoie les données dans une table PostgreSQL (`tickets_analysis`)
- Utilisé comme source principale de Metabase

---

## 📊 7. Visualisation (Metabase)

- Connexion à PostgreSQL
- Création de dashboards dynamiques sans code
- Requêtes directes sur les tables PostgreSQL (ou DuckDB en local)

---

## 🔁 8. Arrêt et nettoyage

```bash
./arreter_stack.bat
```
- Stoppe les conteneurs
- Réinitialise `./data/`

---

## ✅ Final
> Ce projet permet de simuler un flux de tickets clients, les transformer automatiquement, et les analyser dans une interface simple — le tout sans cloud, en full local, et sans action manuelle après le lancement.

