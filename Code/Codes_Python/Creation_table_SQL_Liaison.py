import psycopg2
from tqdm import tqdm
import tempfile

# Paramètres de la base de données
db_params = {
    'host': '188.166.105.53',
    'port': 65001,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'LearnPostgreSQL'
}

# Connexion à la base de données
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Charger tous les types de la table types_de_poi dans un dictionnaire
cur.execute("SELECT id, classe FROM types_de_poi")
type_dict = {classe: id_type for id_type, classe in cur.fetchall()}

# Charger toutes les lignes de la table datatourisme
cur.execute("SELECT id, type FROM datatourisme")
rows = cur.fetchall()

# Préparer les insertions dans la table de liaison
liaison_data = []

# Ajouter une barre de progression pour le traitement des lignes
for row in tqdm(rows, desc="Traitement des lignes de datatourisme"):
    id_datatourisme, types = row
    
    # Séparation des types par des virgules
    types_list = [t.strip() for t in types.split(',')]
    
    # Vérification pour chaque type
    for t in types_list:
        # Si le type est trouvé dans le dictionnaire
        if t in type_dict:
            id_type_de_poi = type_dict[t]
            # Ajouter à la liste des insertions
            liaison_data.append((id_datatourisme, id_type_de_poi))

# Utiliser COPY au lieu de INSERT (plus rapide pour inserer beaucoup de data)
with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
    for record in tqdm(liaison_data, desc="Préparation des données pour COPY"):
        f.write(f"{record[0]}\t{record[1]}\n")
    temp_file_name = f.name

# Copier les données depuis le fichier temporaire
with open(temp_file_name, 'r') as f:
    cur.copy_from(f, 'liaison_datatourisme_types_de_poi', sep='\t', columns=('id_datatourisme', 'id_type_de_poi'))


conn.commit()
conn.close()