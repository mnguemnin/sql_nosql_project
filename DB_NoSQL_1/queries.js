// queries.js

// Afficher tous les documents
print("Afficher tous les documents :");
printjson(db.college_effectifs.find().toArray());

// Afficher les effectifs des élèves de 4ème
print("Afficher les effectifs des élèves de 4ème :");
printjson(db.college_effectifs.find({}, { nombre_total_de_4emes: 1, _id: 0 }).toArray());

// Afficher les effectifs des élèves de 3ème par sexe
print("Afficher les effectifs des élèves de 3ème par sexe :");
printjson(db.college_effectifs.find({}, { '3eme_filles': 1, '3emes_garcons': 1, _id: 0 }).toArray());

// Afficher les effectifs des élèves de 4ème par langue vivante 1
print("Afficher les effectifs des élèves de 4ème par langue vivante 1 :");
printjson(db.college_effectifs.find({}, { nombre_de_4emes_lv1_allemand: 1, nombre_de_4emes_lv1_anglais: 1, nombre_de_4emes_lv1_espagnol: 1, nombre_de_4emes_lv1_autres_langues: 1, _id: 0 }).toArray());

// Afficher les effectifs des élèves de 3ème par langue vivante 2
print("Afficher les effectifs des élèves de 3ème par langue vivante 2 :");
printjson(db.college_effectifs.find({}, { nombre_de_3emes_lv2_allemand: 1, nombre_de_3emes_lv2_anglais: 1, nombre_de_3emes_lv2_espagnol: 1, nombre_de_3emes_lv2_italien: 1, nombre_de_3emes_lv2_autres_langues: 1, _id: 0 }).toArray());
