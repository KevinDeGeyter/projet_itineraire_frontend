import argparse
import psycopg2
import subprocess
from geopy.distance import geodesic
from neo4j import GraphDatabase
from sklearn.cluster import KMeans


parser = argparse.ArgumentParser(description='Script pour créer des clusters de Poins d_interet en fonction de la localisation et du type d_activité.')
parser.add_argument('--latitude', type=float, help='Latitude du point de référence')
parser.add_argument('--longitude', type=float, help='Longitude du point de référence')
parser.add_argument('--poi_types', nargs='+', help='Types d_activité')
args = parser.parse_args()


# Fonction pour filtrer les points d'intérêt

def filter_pois(position, pois, radius_km):
    list_pois = []
    for poi in pois:
        poi_latitude, poi_longitude = float(poi[1]), float(poi[2])
        if -90 <= poi_latitude <= 90 and -180 <= poi_longitude <= 180:
            poi_position = (poi_latitude, poi_longitude)
            if geodesic(position, poi_position).kilometers <= radius_km:
                list_pois .append(poi)
        else:
            print("Coordonées incorrectes")
    return list_pois

# Connexion à la DB PostgreSQL
conn = psycopg2.connect(
    host="188.166.105.53",
    port="65001",
    database="postgres",
    user="postgres",
    password="LearnPostgreSQL"
)
cursor = conn.cursor()

# Requete SQL
poi_types_condition = " OR ".join([f"tp.type = '{poi_type}'" for poi_type in args.poi_types])
sql_query = (
    "SELECT dt.label_fr, dt.latitude, dt.longitude, tp.type "
    "FROM datatourisme dt "
    "JOIN liaison_datatourisme_types_de_poi ldtp ON dt.id = ldtp.id_datatourisme "
    "JOIN types_de_poi tp ON ldtp.id_type_de_poi = tp.id "
    f"WHERE {poi_types_condition} "
    "GROUP BY dt.label_fr, dt.latitude, dt.longitude, tp.type"
)

# Exécution de la requête SQL
cursor.execute(sql_query)
rows = cursor.fetchall()
conn.commit()

# Position de référence
reference_position = (args.latitude, args.longitude)

# Récuperer les points d'interet dans un rayon de km autour du point de référence
list_pois = filter_pois(reference_position, rows, 50)



# utilisation de KMeans pour regrouper les poi en clusters

X = [(row[1], row[2]) for row in list_pois]
kmeans = KMeans(n_clusters=10, n_init=10)
kmeans.fit(X)
clusters = kmeans.labels_

# utilisation de Neoj pour créer une base de donnée orienté graph
uri = "bolt://127.0.0.1:7687"
username = "neo4j"
password = "neo4j"
driver = GraphDatabase.driver(uri, auth=(username, password))

def create_graph(tx):
    # Vérifier l'existence des nœuds Cluster
    existing_clusters = set(tx.run("MATCH (c:Cluster) RETURN c.name").value())
    
    # Création des clusters
    for i in range(max(clusters) + 1):
        cluster_name = f"Cluster_{i}"
        if cluster_name not in existing_clusters:
            tx.run("CREATE (c:Cluster {name: $name})", name=cluster_name)

    # Création des points dintérêt (POI) et des relations avec les clusters
    for i, row in enumerate(list_pois):
        label_fr, latitude, longitude, poi_type = row
        cluster_name = f"Cluster_{clusters[i]}"
        tx.run(
            "CREATE (poi:POI {label_fr: $label_fr, latitude: $latitude, longitude: $longitude, poi_type: $poi_type})",
              label_fr=label_fr, latitude=latitude, longitude=longitude, poi_type=poi_type
        )
        tx.run(
            "MATCH (poi:POI {label_fr: $label_fr}), (cluster:Cluster {name: $cluster_name}) "
            "CREATE (poi)-[:BELONGS_TO]->(cluster)",
            label_fr=label_fr, cluster_name=cluster_name
        )

# Création de la session Neo4j et exécution de la transaction
with driver.session() as session:
    session.write_transaction(create_graph)

# Fermeture du curseur et de la connexion à la base de données PostgreSQL
cursor.close()
conn.close()
# Exécuter le script AfficherCarte.py
subprocess.run(["python3", "AfficherCarte.py"])
