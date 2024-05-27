import streamlit as st
import subprocess

# Définition des paramètres par défaut
default_latitude = 45.12
default_longitude = 5.33
default_poi_types = ["Monument"]

st.title("Paramètres de la requête")

# Sélection de la latitude
latitude = st.number_input("Latitude :", value=default_latitude)

# Sélection de la longitude
longitude = st.number_input("Longitude :", value=default_longitude)

# Liste étendue des types de points d'intérêt
extended_poi_types = [
    "Culture", "Religion", "Sport", "Loisir", "Divertissement", "Hebergement", 
    "Restauration", "Boisson", "Banque", "Hebergement", "Autre", "Plage", 
    "Mobilité réduite", "Moyen de locomotion", "Montagne", "Antiquité", 
    "Histoire", "Musée", "Détente", "Bar", "Commerce local", "Point de vue", 
    "Nature", "Camping", "Cours d'eau", "Service", "Monument", "Jeunesse", 
    "Apprentissage", "Marché", "Vélo", "Magasin", "Animaux", "Location", 
    "Parcours", "Santé", "Information", "Militaire", "Parking", 
    "Marche à pied", "POI", "Piscine"
]

# Sélection des types de points d'intérêt (POI) avec des cases à cocher
poi_types = st.multiselect("Types de points d'intérêt :", extended_poi_types, default=default_poi_types)

# Bouton pour exécuter la requête
if st.button("Exécuter la requête"):
    # Construction de la commande à exécuter
    poi_types_str = " ".join(poi_types)
    command = f"python3 Creation_Clusters.py --latitude {latitude} --longitude {longitude} --poi_types {poi_types_str}"

    # Exécution de la commande dans un sous-processus
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    # Affichage du résultat de l'exécution
    if process.returncode == 0:
        st.success("La requête a été exécutée avec succès !")
        # Afficher clusters_map_with_tooltips.html dans un encadré
        st.markdown("## Résultat de la carte")
        st.markdown(
            '<iframe src="http://99.81.159.66/clusters_map_with_tooltips.html" width="100%" height="600" frameborder="0"></iframe>',
            unsafe_allow_html=True
        )
    else:
        st.error(f"Erreur lors de l'exécution de la requête : {stderr.decode('utf-8')}")
