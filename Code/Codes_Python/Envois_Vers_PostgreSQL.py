import psycopg2
from psycopg2 import sql
import csv

def truncate_value(value, max_length):
    return value[:max_length] if len(value) > max_length else value

def round_coordinate(value, decimal_places):
    try:
        return f"{round(float(value), decimal_places):.{decimal_places}f}"
    except ValueError:
        return value

# Paramètres de DB PostgreSQL
db_params = {
    'host': '188.166.105.53',
    'port': 65001,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'LearnPostgreSQL'
}

table_data = []
with open('output_data.csv', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        table_data.append(row)
        
# Connexion à la DB et création de la table
print("Connexion à la base de données...")
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

print("Création de la table DataTourisme...")
create_table_query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS DataTourisme (
        id SERIAL PRIMARY KEY,
        uri VARCHAR(1000),
        label_fr VARCHAR(1000),
        description_fr TEXT,
        telephone VARCHAR(255),
        adresse VARCHAR(1000),
        ville VARCHAR(255),
        latitude VARCHAR(255),
        longitude VARCHAR(255),
        type TEXT
    );
""")

cursor.execute(create_table_query)
print("Table DataTourisme créée avec succès.")

# Insertion des données du CSV dans la table Datatourisme
print("Insertion des données dans la table...")
insert_query = sql.SQL("""
    INSERT INTO DataTourisme(uri, label_fr, description_fr, telephone, adresse, ville, latitude, longitude, type)
    VALUES (%(uri)s, %(label_fr)s, %(description_fr)s, %(telephone)s, %(adresse)s, %(ville)s, %(latitude)s, %(longitude)s, %(type)s);
""")

batch_size = 100
total_records = len(table_data)
inserted_records = 0

try:
    for i in range(0, total_records, batch_size):
        batch_data = table_data[i:i+batch_size]
        for data in batch_data:
            uri = truncate_value(data['URI'], 1000)
            label_fr = truncate_value(data['Label (fr)'], 1000)
            description_fr = truncate_value(data['ShortDescription_fr'], 1000)
            telephone = truncate_value(data['Téléphone'], 255)
            adresse = truncate_value(data['Adresse'], 1000)
            ville = truncate_value(data['Ville'], 255)
            latitude = round_coordinate(data['Latitude'], 6)
            longitude = round_coordinate(data['Longitude'], 6)
            type = truncate_value(data['Type'], 1000)

            cursor.execute(insert_query, {
                'uri': uri,
                'label_fr': label_fr,
                'description_fr': description_fr,
                'telephone': telephone,
                'adresse': adresse,
                'ville': ville,
                'latitude': latitude,
                'longitude': longitude,
                'type': type
            })
            inserted_records += 1
            percentage_complete = (inserted_records / total_records) * 100
            print(f"Insertion en cours... {inserted_records}/{total_records} ({percentage_complete:.2f}%)", end="\r")
        conn.commit()
except Exception as e:
    conn.rollback() 
    print(f"Erreur lors de l'insertion des données : {e}")

print("\nFermeture de la connexion...")
cursor.close()
conn.close()
print("Connexion fermée.")
