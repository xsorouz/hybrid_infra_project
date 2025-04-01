# Guide : Commandes Docker pour exécuter le projet Infrastructure Hybride

Ce document vous guide étape par étape pour **construire une image Docker personnalisée**, **tester vos services** et **intégrer vos scripts Python** dans un conteneur dédié.

---

## Étape 1 : Lancer l'infrastructure via `docker-compose`

Assurez-vous d’avoir Docker installé sur votre machine.

Dans le dossier du projet :

```bash
docker-compose up -d
```

Cela démarre les services suivants :
- Redpanda (port 9092)
- MinIO (port 9000 + console 9001)
- ClickHouse (port 8123)
- PostgreSQL (port 5432)

---

## Étape 2 : Construire l'image Docker de vos scripts Python

À la racine du projet, où se trouvent le `Dockerfile` et `requirements.txt`, exécutez :

```bash
docker build -t infra-tests .
```

Cela va :
1. Télécharger l'image Python
2. Copier vos fichiers scripts et `requirements.txt`
3. Installer automatiquement les dépendances
4. Configurer le script `test_all_services.py` comme point d’entrée

---

## Étape 3 : Exécuter le conteneur de test

Pour tester les services à partir de vos scripts Python dans un conteneur :

```bash
docker run --network=infra_hybride_default infra-tests
```

> **Important** : L’option `--network=infra_hybride_default` permet à votre conteneur d’accéder aux services `Redpanda`, `MinIO`, etc., créés par `docker-compose`.

---

## Étape 4 : Accéder aux interfaces graphiques

| Service     | URL locale                    | Identifiants                    |
|-------------|-------------------------------|---------------------------------|
| MinIO       | http://localhost:9001         | admin / admin123               |
| ClickHouse  | http://localhost:8123         | anonyme                        |
| Redpanda    | http://localhost:9644         | API admin (pas d’interface UI) |
| PostgreSQL  | localhost:5432 (via DBeaver)  | user / pass                    |

---

## Étape 5 : Debug & autres commandes utiles

### Redémarrer un service :
```bash
docker-compose restart clickhouse
```

### Lister les conteneurs actifs :
```bash
docker ps
```

### Supprimer tous les conteneurs et volumes :
```bash
docker-compose down -v
```

---

## Nettoyage (facultatif)

Si vous souhaitez supprimer l'image créée :

```bash
docker rmi infra-tests
```

---

Ce guide vous permet de simuler une architecture cloud hybride **entièrement localement**, avec des composants prêts pour l’intégration d’un pipeline ETL/Streaming.



---

## Hypothèses sur votre environnement Docker

Les commandes fournies fonctionnent que vous ayez ou non déjà utilisé Docker.

- **Pas besoin d’avoir des conteneurs ou images préinstallés** : Docker téléchargera et créera automatiquement ce qu’il faut lors du `docker-compose up`.
- **Les réseaux et volumes Docker seront créés automatiquement** s’ils n’existent pas.
- **Si vous avez déjà utilisé MinIO, Redpanda, etc.**, leurs conteneurs ne seront pas affectés sauf si leurs noms entrent en conflit avec ceux de ce projet.

---

## Nettoyage (réinitialisation complète)

Si vous souhaitez repartir de zéro proprement :

```bash
# Stopper et supprimer les conteneurs + volumes
docker-compose down -v

# Supprimer l’image Docker personnalisée pour les tests Python
docker rmi infra-tests

# Supprimer tous les volumes Docker non utilisés (attention : global)
docker volume prune
```

Ces commandes sont utiles si vous changez de configuration ou si vous voulez faire de la place sur votre machine.

---

## Conseils

- Pensez à utiliser `docker ps` pour vérifier que vos conteneurs sont bien en cours d'exécution.
- Vous pouvez relancer `docker-compose up -d` à tout moment pour redémarrer l’infrastructure.

