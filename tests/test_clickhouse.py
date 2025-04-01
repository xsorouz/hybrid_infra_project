"""
Script de test de connexion à ClickHouse.
➡️ Utilise la lib `clickhouse_driver`
➡️ Se connecte à ClickHouse en local via Docker (port 9009 remappé)
➡️ Crée une base, une table, insère une ligne, puis affiche le résultat.
"""

from clickhouse_driver import Client
import os
from dotenv import load_dotenv

# ✅ Chargement des variables d’environnement depuis le fichier .env
load_dotenv()

# 🔐 Récupération des identifiants ClickHouse (ou valeurs par défaut si absents)
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

# ✅ Connexion à ClickHouse (port mappé en 9009)
client = Client(
    host='localhost',
    port=9009,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD
)

# 🛠️ Requêtes de test : création base + table + insertion
client.execute('CREATE DATABASE IF NOT EXISTS testdb')
client.execute('''
    CREATE TABLE IF NOT EXISTS testdb.hello (
        id UInt32,
        msg String
    ) ENGINE = Memory
''')
client.execute("INSERT INTO testdb.hello VALUES (1, 'Hello from ClickHouse!')")

# ✅ Requête de lecture
result = client.execute('SELECT * FROM testdb.hello')
print("✔ Résultat ClickHouse :", result)
