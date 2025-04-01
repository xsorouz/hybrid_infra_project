"""
Script de test de connexion à PostgreSQL.
➡️ Utilise psycopg2 pour se connecter à une base locale Dockerisée
➡️ Crée une table, insère une ligne et affiche le contenu.
"""

import psycopg2
import time

# 🔁 Attente du démarrage de PostgreSQL (max 10 tentatives)
for attempt in range(30):
    try:
        # ✅ Connexion à PostgreSQL avec les identifiants définis dans docker-compose
        conn = psycopg2.connect(
            dbname="demo_db",       # 🧪 Nom de la base définie dans POSTGRES_DB
            user="user",            # 👤 Utilisateur PostgreSQL
            password="pass",        # 🔐 Mot de passe
            host="localhost",       # 📍 Hôte local (via Docker)
            port=5432               # 🌐 Port par défaut
        )
        print("✔ Connexion à PostgreSQL réussie")
        break  # ✅ Si la connexion fonctionne, on sort de la boucle
    except psycopg2.OperationalError:
        print(f"⏳ Tentative {attempt+1}/30 en attente de PostgreSQL...")
        time.sleep(1)
else:
    raise Exception("✘ Échec de la connexion à PostgreSQL après plusieurs tentatives")

# ✅ Création d’un curseur SQL
cur = conn.cursor()

# 🛠️ Création de table si elle n’existe pas
cur.execute("""
    CREATE TABLE IF NOT EXISTS greetings (
        id SERIAL PRIMARY KEY,
        message TEXT
    );
""")

# ✉️ Insertion d’un message
cur.execute("INSERT INTO greetings (message) VALUES (%s)", ("Hello from PostgreSQL!",))

# 💾 Sauvegarde des modifications
conn.commit()

# 📥 Lecture et affichage du contenu
cur.execute("SELECT * FROM greetings;")
rows = cur.fetchall()
print("✔ Résultats PostgreSQL :", rows)

# ✅ Fermeture propre
cur.close()
conn.close()
