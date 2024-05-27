import folium
from neo4j import GraphDatabase

# Connexion à la base de données Neo4j
uri = "bolt://127.0.0.1:7687"
username = "neo4j"
password = "neo4j"
driver = GraphDatabase.driver(uri, auth=(username, password))

# Fonction pour récupérer les coordonnées GPS et les labels des POIs de chaque cluster depuis Neo4j
def get_clusters_poi_data(min_poi_count=6, max_poi_per_cluster=10, max_clusters=10):
    clusters_data = {}
    with driver.session() as session:
        result = session.run(
            """
            MATCH (c:Cluster)<-[:BELONGS_TO]-(p:POI)
            WITH c, count(p) AS poi_count, collect([p.latitude, p.longitude, p.label_fr]) AS poi_data
            WHERE poi_count >= $min_poi_count
            RETURN c.name AS cluster_name, poi_data
            LIMIT $max_clusters
            """,
            min_poi_count=min_poi_count,
            max_clusters=max_clusters
        )
        for record in result:
            cluster_name = record["cluster_name"]
            poi_data = record["poi_data"][:max_poi_per_cluster]  # Limiter le nombre de POIs par cluster
            clusters_data[cluster_name] = poi_data
    return clusters_data

# Récupérer les données des POIs pour les clusters avec au moins 6 POI et au maximum 10 clusters
clusters_data = get_clusters_poi_data(min_poi_count=6, max_poi_per_cluster=10, max_clusters=10)

# Créer la carte Google Maps
map = folium.Map(location=[0, 0], zoom_start=2)

# Définir les couleurs pour les marqueurs de chaque cluster
colors = ['red', 'blue', 'green', 'purple', 'orange', 'lightgreen', 'pink', 'white', 'gray', 'black']
# Ajouter un marqueur pour chaque POI de chaque cluster avec une couleur différente
for i, (cluster_name, poi_data) in enumerate(clusters_data.items()):
    color = colors[i % len(colors)]  # Utilisation d'une couleur cyclique pour chaque cluster
    for poi_coordinate in poi_data:
        latitude, longitude, label_fr = poi_coordinate  # Modification ici pour déballer les valeurs correctement
        # Ajouter un marqueur avec une info-bulle (tooltip) pour afficher le label_fr du POI
        folium.Marker(
            location=[latitude, longitude],
            icon=folium.Icon(color=color),
            tooltip=label_fr  # Afficher le label_fr du POI lors du survol du point
        ).add_to(map)

# Sauvegarder la carte dans un fichier HTML
map.save("clusters_map_with_tooltips.html")