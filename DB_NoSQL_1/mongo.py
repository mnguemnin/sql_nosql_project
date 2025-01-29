import csv
from pymongo import MongoClient

# ###   Connexion à MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["DB_NoSQL_1"]  # Nom de la base de données
collection = db["data_collection"]  

# Lire le fichier CSV en gérant le BOM et le bon séparateur
with open("data.csv", "r", encoding="utf-8-sig") as file:
    reader = csv.DictReader(file, delimiter=";")  # Définition du bon séparateur
    data = []
    
    for row in reader:
        cleaned_row = {key.strip(): (value if value is not None else "") for key, value in row.items()}  # Nettoyage
        data.append(cleaned_row)

# Insérer les données dans MongoDB
if data:
    collection.insert_many(data)
    print("✅ Importation terminée avec succès dans DB_NoSQL_1.")
else:
    print("⚠️ Aucune donnée trouvée dans le fichier CSV.")