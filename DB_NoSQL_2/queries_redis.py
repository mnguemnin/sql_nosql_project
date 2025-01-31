import redis

# Connexion à Redis et sélection de la base de données 1 (DB_NoSQL_2)
r = redis.StrictRedis(host='localhost', port=6379, db=1)

# Exemple de requêtes
def query_redis(key, value):
    keys = r.keys('*')
    results = []
    for k in keys:
        data = r.hgetall(k)
        if key in data and data[key].decode('utf-8') == value:
            results.append(data)
    return results

# Afficher les activations pour les bénéficiaires de genre 'F'
print("Afficher les activations pour les bénéficiaires de genre 'F' :")
results = query_redis('beneficiaire_genre', 'F')
for result in results:
    print(result)

# Afficher les activations pour les bénéficiaires de la région 'Île-de-France'
print("\nAfficher les activations pour les bénéficiaires de la région 'Île-de-France' :")
results = query_redis('region', 'Île-de-France')
for result in results:
    print(result)

# Afficher les activations pour les bénéficiaires de la fédération 'FÉDÉRATION FRANÇAISE DE FOOTBALL'
print("\nAfficher les activations pour les bénéficiaires de la fédération 'FÉDÉRATION FRANÇAISE DE FOOTBALL' :")
results = query_redis('federation', 'FÉDÉRATION FRANÇAISE DE FOOTBALL')
for result in results:
    print(result)

# Afficher les activations pour les bénéficiaires âgés de 10 ans
print("\nAfficher les activations pour les bénéficiaires âgés de 10 ans :")
results = query_redis('beneficiaire_age', '10')
for result in results:
    print(result)
