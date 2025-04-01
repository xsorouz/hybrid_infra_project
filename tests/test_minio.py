"""
Script de test de connexion à MinIO.
➡️ Utilise la librairie officielle `minio`
➡️ Se connecte à MinIO en local via Docker (port 9000)
➡️ Crée un bucket si nécessaire, puis y envoie un fichier texte.
"""

from minio import Minio
from minio.error import S3Error
import io  # 📦 Permet d’envoyer un contenu binaire depuis la mémoire

# ✅ Connexion au serveur MinIO local (config par défaut définie dans .env ou docker-compose)
client = Minio(
    "localhost:9000",           # 🎯 Adresse et port du serveur MinIO (exposé dans docker-compose)
    access_key="admin",         # 🔐 Identifiant (correspond à MINIO_ROOT_USER dans .env)
    secret_key="admin123",      # 🔐 Mot de passe (correspond à MINIO_ROOT_PASSWORD dans .env)
    secure=False                # ❗ False car HTTP (pas HTTPS)
)

# 📁 Nom du bucket à utiliser
bucket_name = "test-bucket"

# 📝 Nom et contenu du fichier à envoyer
object_name = "hello.txt"
file_content = b"Hello from MinIO!"  # b"" signifie contenu binaire

try:
    # ✅ Crée le bucket s’il n’existe pas déjà
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"✔ Bucket '{bucket_name}' créé avec succès")
    else:
        print(f"✔ Bucket '{bucket_name}' déjà existant")

    # ✅ Envoie du fichier vers le bucket
    client.put_object(
        bucket_name,
        object_name,
        data=io.BytesIO(file_content),  # 🔁 Stream binaire mémoire
        length=len(file_content)        # 🧮 Taille exacte du contenu
    )
    print(f"✔ Fichier '{object_name}' envoyé dans le bucket '{bucket_name}'")

except S3Error as err:
    print(f"✘ Erreur MinIO : {err}")
