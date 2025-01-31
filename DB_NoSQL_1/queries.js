// queries.js

// Afficher tous les documents
print("Afficher tous les documents :");
printjson(db.data_collection.find().toArray());

// Afficher les activations pour les bénéficiaires de genre 'F'
print("Afficher les activations pour les bénéficiaires de genre 'F' :");
printjson(db.data_collection.find({ beneficiaire_genre: 'F' }).toArray());

// Afficher les activations pour les bénéficiaires de la région 'Île-de-France'
print("Afficher les activations pour les bénéficiaires de la région 'Île-de-France' :");
printjson(db.data_collection.find({ region: 'Île-de-France' }).toArray());

// Afficher les activations pour les bénéficiaires de la fédération 'FÉDÉRATION FRANÇAISE DE FOOTBALL'
print("Afficher les activations pour les bénéficiaires de la fédération 'FÉDÉRATION FRANÇAISE DE FOOTBALL' :");
printjson(db.data_collection.find({ federation: 'FÉDÉRATION FRANÇAISE DE FOOTBALL' }).toArray());

// Afficher les activations pour les bénéficiaires âgés de 10 ans
print("Afficher les activations pour les bénéficiaires âgés de 10 ans :");
printjson(db.data_collection.find({ beneficiaire_age: '10' }).toArray());
