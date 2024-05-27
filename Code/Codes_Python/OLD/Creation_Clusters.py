from neo4j import GraphDatabase
from sklearn.cluster import KMeans
import psycopg2
from geopy.distance import geodesic

# Fonction pour filtrer les points d'intérêt (POI) dans un rayon donné autour d'une position géographique

def filter_pois(position, pois, radius_km)
    filtered_pois = []
    for poi in pois
        poi_latitude, poi_longitude = float(poi[1]), float(poi[2])
        # Ensure latitude is within the valid range
        if -90 = poi_latitude = 90
            poi_position = (poi_latitude, poi_longitude)
            if geodesic(position, poi_position).kilometers = radius_km
                filtered_pois.append(poi)
        else
            print(fLatitude out of range for POI {poi})
    return filtered_pois

# Connexion à la base de données PostgreSQL
conn = psycopg2.connect(
    host=188.166.105.53,
    port=65001,
    database=postgres,
    user=postgres,
    password=LearnPostgreSQL
)
cursor = conn.cursor()

# Récupération des données depuis la table datatourisme
cursor.execute(SELECT label_fr, latitude, longitude FROM datatourisme)
rows = cursor.fetchall()
conn.commit() 

# Position choisie pour le test
reference_position = (48.2115397, 6.7204365)  

# POIs dans un rayon de 50 km autour de la position
filtered_pois = filter_pois(reference_position, rows, 50)

# Création de la matrice de caractéristiques pour l'algorithme K-Means
X = [(row[1], row[2]) for row in filtered_pois]  

# Application de K-Means pour regrouper les points d'intérêt (POI) en clusters
kmeans = KMeans(n_clusters=len(filtered_pois)  10, n_init=10)  # Calcul du nombre de clusters en fonction du nombre de POI
kmeans.fit(X)
clusters = kmeans.labels_

# Connexion à la base de données Neo4j
uri = bolt127.0.0.17687  
username = neo4j
password = neo4j
driver = GraphDatabase.driver(uri, auth=(username, password))

# Fonction pour créer le graphe dans Neo4j
def create_graph(tx)
    # Vérifier l'existence des nœuds Cluster
    existing_clusters = set(tx.run(MATCH (cCluster) RETURN c.name).value())
    
    # Création des clusters
    for i in range(max(clusters) + 1)
        cluster_name = fCluster_{i}
        if cluster_name not in existing_clusters
            tx.run(CREATE (Cluster {name $name}), name=cluster_name)

    # Création des points d'intérêt (POI) et des relations avec les clusters
    for i, row in enumerate(filtered_pois)
        label_fr, latitude, longitude = row
        cluster_name = fCluster_{clusters[i]}
        tx.run(
            
            CREATE (aPOI {
                label_fr $label_fr,
                latitude $latitude,
                longitude $longitude
            })
            ,
 	label_fr=label_fr, latitude=latitude, longitude=longitude
        )
        tx.run(
            
            MATCH (pPOI {label_fr $label_fr}), (cCluster {name $cluster_name})
            CREATE (p)-[BELONGS_TO]-(c)
            ,
            label_fr=label_fr, cluster_name=cluster_name
        )

# Création de la session Neo4j et exécution de la transaction
with driver.session() as session
    session.write_transaction(create_graph)


cursor.close()
conn.close()
