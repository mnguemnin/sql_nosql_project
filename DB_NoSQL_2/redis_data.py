import csv
import time
import redis

# Connexion à Redis et sélection de la base de données 1 (DB_NoSQL_2)
r = redis.StrictRedis(host='localhost', port=6379, db=1)

# Supprimer toutes les clés existantes dans la base de données Redis
r.flushdb()

# Enregistrer le temps de début
start_time = time.time()

# Lire le fichier CSV en gérant le BOM et le bon séparateur
with open("data.csv", "r", encoding="utf-8-sig") as file:
    reader = csv.DictReader(file, delimiter=",")  # Définition du bon séparateur
    for row in reader:
        cleaned_row = {key.strip(): value.strip() for key, value in row.items()}  # Nettoyage
        # Utiliser un identifiant unique pour chaque enregistrement (par exemple, une combinaison de champs)
        unique_id = f"{cleaned_row['beneficiaire_age']}_{cleaned_row['beneficiaire_genre']}_{cleaned_row['organisme']}_{cleaned_row['date_recours_pass_sport']}"
        r.hset(unique_id, mapping=cleaned_row)

# Enregistrer le temps de fin
end_time = time.time()

# Calculer le temps écoulé
elapsed_time = end_time - start_time
print(f"Temps d'importation des données : {elapsed_time:.2f} secondes")
