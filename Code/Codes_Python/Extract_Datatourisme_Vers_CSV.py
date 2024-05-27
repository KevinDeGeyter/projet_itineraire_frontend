import os
import pandas as pd
import json
from tqdm.notebook import tqdm

index_file_path = "index.json"

# Récuperer les données des fichiers créés depuis l'extract DataTourisme
with open(index_file_path, 'r', encoding='utf-8') as index_file:
    index_data = json.load(index_file)

# Création d'une liste pour stocker les données
table_data = []

# Parcourir les données pour exporter les informations utiles dans un fichier csv
for file_info in tqdm(index_data, desc="Traitement des fichiers"):
    file_path = os.path.join(os.path.dirname(index_file_path), "objects", file_info['file'])

    with open(file_path, 'r', encoding='utf-8', errors='replace') as json_file:
        json_data = json.load(json_file)

        # Récupérer les données correctement
        label_fr = json_data.get('rdfs:label', {}).get('fr', '')
        if isinstance(label_fr, list):
            label_fr = ', '.join(label_fr)

        telephone = json_data.get('hasContact', [{}])[0].get('schema:telephone', '')
        if isinstance(telephone, list):
            telephone = ', '.join(telephone)

        location_info = json_data.get('isLocatedAt', [{}])[0]
        address_info = location_info.get('schema:address', [{}])[0]
        address_locality = address_info.get('schema:addressLocality', '')
        if isinstance(address_locality, list):
            address_locality = ', '.join(address_locality)

        geo_info = location_info.get('schema:geo', {})
        latitude = geo_info.get('schema:latitude', '')
        if isinstance(latitude, list):
            latitude = ', '.join(latitude)

        longitude = geo_info.get('schema:longitude', '')
        if isinstance(longitude, list):
            longitude = ', '.join(longitude)

        short_description_fr = json_data.get('hasDescription', [{}])[0].get('shortDescription', {}).get('fr', '')
        if isinstance(short_description_fr, list):
            short_description_fr = ', '.join(short_description_fr)

        uri = json_data.get('@id', '')
        if isinstance(uri, list):
            uri = ', '.join(uri)

        types = json_data.get('@type', [])
        if isinstance(types, list):
            types = ', '.join(types)

        table_data.append({
            'URI': uri,
            'Label (fr)': label_fr,
            'Téléphone': telephone,
            'Adresse': address_info.get('schema:streetAddress', ''),
            'Ville': address_locality,
            'Latitude': latitude,
            'Longitude': longitude,
            'ShortDescription_fr': short_description_fr,  
            'Type': types 
        })

# Convertir la liste en DataFrame
df = pd.DataFrame(table_data)

# Enregistrer le DataFrame au format CSV
output_csv_path = "output_data.csv"
df.to_csv(output_csv_path, index=False)

print(f"Les données ont été enregistrées dans le fichier : {output_csv_path}")
