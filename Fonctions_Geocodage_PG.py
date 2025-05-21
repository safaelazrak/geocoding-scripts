import json
import time

import requests
import re
from  Ressources_Geocodage import get_cp_non_geocode_query, get_coord_cp_ref_query, update_cp_geocode_query, NbDeCallAPIGouv, select_cp_non_corrige_query

#Corrige les codes postaux pour s'assurer qu'ils sont valides.
def corriger_code_postal(cp):
    try:
        if cp is None:
            return None
        cp = str(cp).zfill(5)
        regex = r'^(0[1-9]|[1-8][0-9]|9[0-5])\d{3}$'
        if not re.match(regex, cp):
            print(f"Code postal invalide : {cp}")
            return None
        return cp
    except Exception as e:
        print(f"Erreur dans corriger_code_postal (Code postal : {cp}) : {e}")
        return None

#Appliquer les modifications sur les codes postaux
def corriger_codes_postaux(cur, update_query, conn):
    # Requête pour ne sélectionner que les lignes non géocodées
    cur.execute(select_cp_non_corrige_query)
    ListeClients = cur.fetchall()

    for client in ListeClients:
        id_client = client[0]
        code_postal = client[1]

        # Corriger le code postal si nécessaire
        nouveau_code_postal = corriger_code_postal(code_postal)

        if nouveau_code_postal != code_postal:
            print(f"Code postal {code_postal} corrigé en {nouveau_code_postal} pour l'ID {id_client}.")
            cur.execute(update_query, (nouveau_code_postal, id_client))

            # Marquer la ligne comme géocodée après la correction
            cur.execute("""UPDATE public."Adresses_Client" SET "Is_Geocoded" = TRUE WHERE "PHX_NumeroICU__c" = %s """, (id_client,))
            conn.commit()
        else:
            print(f"Code postal {code_postal} déjà correct pour l'ID {id_client}.")

#Concatène les champs d'adresse en une seule chaîne
def concatener_adresse(street, city, postal_code):
    return f"{street}, {city}, {postal_code}"

#Nettoie une adresse en supprimant les caractères inutiles
def nettoyer_adresse(adresse):
    try:
        adresse_nettoyee = re.sub(r'[^\w\s,]', '', adresse)
        adresse_nettoyee = re.sub(r'\s+', ' ', adresse_nettoyee).strip()
        return adresse_nettoyee
    except Exception as e:
        print(f"Erreur lors du nettoyage de l'adresse : {adresse} - {e}")
        return adresse

#Géocode une adresse complète
def geocoder_adresse(adresse):
    try:
        adresse = nettoyer_adresse(adresse)
        NbDeCall()
        url = f"https://api-adresse.data.gouv.fr/search/?q={adresse}&limit=1"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data['features']:
                feature = data['features'][0]
                latitude = feature['geometry']['coordinates'][1]
                longitude = feature['geometry']['coordinates'][0]
                geometry = {
                    "type": "Point",
                    "coordinates": [longitude, latitude]
                }
                return latitude, longitude, geometry
        print(f"Pas de résultats pour l'adresse : {adresse}")
    except Exception as e:
        print(f"Erreur lors du géocodage de l'adresse {adresse} : {e}")
    return None, None, None

def mettre_a_jour_adresses(ListeClients, cur, update_address_query, conn):
    mises_a_jour = []

    for adresseclient in ListeClients:
        street = adresseclient[1]  # "PersonMailingAddress.street"
        city = adresseclient[2]  # "PersonMailingAddress.city"
        postal_code = adresseclient[3]  # "PersonMailingAddress.postalCode"
        address = concatener_adresse(street, city, postal_code)
        mises_a_jour.append((address, adresseclient[0]))  # client[0] est l'ID unique

    if mises_a_jour:
        cur.executemany(update_address_query, mises_a_jour)
        conn.commit()
        print("Adresses complètes mises à jour.")

def mettre_a_jour_geocodage(ListeClients, cur, update_geocode_query, conn):
    # Sélectionner les lignes qui n'ont pas encore été géocodées et qui ont une adresse complète
    cur.execute("""
            SELECT "PHX_NumeroICU__c", "PersonMailingAddress.street", "PersonMailingAddress.city", "PersonMailingAddress.postalCode"
            FROM public."Adresses_Client"
            WHERE "Is_Geocoded" = FALSE
            AND "Adresse_complete" IS NOT NULL;
        """)

    ListeClients = cur.fetchall()  # Récupère toutes les lignes correspondantes
    geocodages = []

    for client in ListeClients:
        adresse = client[1]
        latitude, longitude, geometry = geocoder_adresse(adresse)
        if latitude and longitude:
            latitude_json = json.dumps(latitude)
            longitude_json = json.dumps(longitude)
            geocodages.append((latitude_json, longitude_json, json.dumps(geometry), True, client[0]))
            print(
                f"Coordonnées géographiques mises à jour pour l'ID {client[0]} : Latitude {latitude}, Longitude {longitude}")

    if geocodages:
        cur.executemany(update_geocode_query, geocodages)
        conn.commit()
        print("Coordonnées géographiques mises à jour pour les adresses géocodées.")

#Nettoie un code postal en supprimant les espaces superflu
def nettoyer_code_postal(cp):
    if cp is None:
        return None
    cp = str(cp).strip()
    return cp

#Géocode tous les codes postaux de la table CP_ref
def geocoder_codes_postaux(cur, conn):
    try:
        cur.execute(""" 
            SELECT "Postal_Code"
            FROM public."CP_ref"
            WHERE "Latitude" IS NULL OR "Longitude" IS NULL;
        """)
        codes_postaux = cur.fetchall()
        print(f"Nombre de codes postaux récupérés : {len(codes_postaux)}")

        for code_postal in codes_postaux:
            cp = code_postal[0]
            print(f"Géocodage du code postal {cp}")

            cp_nettoye = nettoyer_code_postal(cp)
            if not cp_nettoye:
                print(f"Code postal {cp} non valide après nettoyage.")
                cur.execute(""" UPDATE public."CP_ref" SET "Is_Error" = True WHERE "Postal_Code" = %s; """, (cp,))
                continue

            try:
                NbDeCall()
                url = f"https://api-adresse.data.gouv.fr/search/?q=8+bd+du+port&postcode={cp_nettoye}"
                response = requests.get(url)

                if response.status_code == 200:
                    data = response.json()

                    if data['features']:
                        feature = data['features'][0]
                        latitude = feature['geometry']['coordinates'][1]
                        longitude = feature['geometry']['coordinates'][0]
                        geometry = {
                            "type": "Point",
                            "coordinates": [longitude, latitude]
                        }

                        latitude_json = json.dumps(latitude)
                        longitude_json = json.dumps(longitude)
                        geometry_json = json.dumps(geometry)

                        update_query = """
                            UPDATE public."CP_ref"
                            SET "Latitude" = %s, "Longitude" = %s, "Geometry" = %s, "Is_Error" = False
                            WHERE "Postal_Code" = %s;
                        """
                        cur.execute(update_query, (latitude_json, longitude_json, geometry_json, cp_nettoye))
                    else:
                        print(f"Aucun résultat pour {cp_nettoye}.")
                        cur.execute(""" UPDATE public."CP_ref" SET "Is_Error" = True WHERE "Postal_Code" = %s; """, (cp_nettoye,))
                else:
                    print(f"Erreur API pour {cp_nettoye}, statut {response.status_code}")
                    cur.execute(""" UPDATE public."CP_ref" SET "Is_Error" = True WHERE "Postal_Code" = %s; """, (cp_nettoye,))
            except Exception as e:
                print(f"Erreur pour {cp_nettoye}: {e}")
                cur.execute(""" UPDATE public."CP_ref" SET "Is_Error" = True WHERE "Postal_Code" = %s; """, (cp_nettoye,))

        conn.commit()
        print("Coordonnées géographiques des codes postaux mises à jour.")
    except Exception as e:
        print(f"Erreur dans la mise à jour des coordonnées des codes postaux : {e}")

#Récupère les codes du département à partir de la table 'CP_ref' où Is_Error = True
def get_department_code(cur):
    cur.execute(""" 
        SELECT "Postal_Code"
        FROM public."CP_ref"
        WHERE "Is_Error" = TRUE
    """)

    postal_codes = cur.fetchall()
    department_codes = [postal_code[0][:2] for postal_code in postal_codes]  # Récupérer seulement les 2 premiers chiffres

    print(f"Codes de départements récupérés: {department_codes}")  # Affiche les codes de départements pour vérification
    return department_codes

#Récupère le code de la première commune pour un département donné
def get_commune_code(department_code):
    NbDeCall()
    url = f'https://geo.api.gouv.fr/departements/{department_code}/communes?fields=nom,code,codesPostaux,siren,codeEpci,codeDepartement,codeRegion,population&format=json&geometry=centre'
    headers = {'accept': 'application/json'}

    print(f"Appel API pour le département {department_code}...")  # Affiche le département avant l'appel API

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        communes = response.json()
        if communes:
            first_commune = communes[0]  # La première commune dans la liste
            commune_code = first_commune['code']
            print(f"Premier code de commune trouvé: {commune_code}")
            return commune_code
        else:
            print(f"Aucune commune trouvée pour le département {department_code}")
            return None
    else:
        print(f"Erreur dans la réponse de l'API pour le département {department_code}: {response.status_code}")
        print(response.text)  # Affiche le corps de la réponse pour plus de détails
        return None

#Géocode une commune à partir de son code postal géocodé et met à jour la base de données avec les coordonnées géographiques pour le code postal initial
def geocode_and_update_db(cur, commune_code, postal_code, conn):
    NbDeCall()
    url = f'https://geo.api.gouv.fr/communes?codePostal={commune_code}&zone=metro&type=commune-actuelle&fields=centre&format=json&geometry=centre'
    headers = {'accept': 'application/json'}

    print(f"Appel à l'API de géocodage pour le code postal {commune_code}...")

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        geocode_data = response.json()
        print(f"Réponse de l'API pour le code postal {commune_code}: {geocode_data}")

        if isinstance(geocode_data, list) and geocode_data:
            first_result = geocode_data[0]
            if 'centre' in first_result:
                centre = first_result['centre']
                coordinates = centre['coordinates']
                longitude = coordinates[0]
                latitude = coordinates[1]

                # Créer un dictionnaire JSON avec les coordonnées et la géométrie
                geometry = {
                    "type": "Point",
                    "coordinates": [longitude, latitude]
                }

                latitude_json = json.dumps( latitude)
                longitude_json = json.dumps(longitude)
                geometry_json = json.dumps(geometry)

                # Mise à jour de la table avec les nouvelles coordonnées géographiques
                print(
                    f"Mise à jour avec : Latitude={latitude}, Longitude={longitude}, Geometry={geometry_json}, Postal_Code={postal_code}")
                update_query = """
                    UPDATE public."CP_ref"
                    SET "Latitude" = %s,
                        "Longitude" = %s,
                        "Geometry" = %s,
                        "Is_Error" = FALSE
                    WHERE "Postal_Code" = %s
                """
                cur.execute(update_query, (latitude_json, longitude_json, geometry_json, postal_code))

                # Validation des changements dans la base de données
                conn.commit()
                print(
                    f"Coordonnées géographiques mises à jour pour le code postal {postal_code}: Latitude={latitude}, Longitude={longitude}")
                return True
            else:
                print(f"Aucune donnée géographique valide trouvée pour le code postal {commune_code}")
                return False
        else:
            print(f"Aucune donnée retournée pour le code postal {commune_code}. Vérifie la validité du code postal.")
            return False
    else:
        print(f"Erreur dans la réponse de l'API pour le code postal {commune_code}: {response.status_code}")
        print(response.text)  # Affiche le corps de la réponse pour plus de détails
        return False

#Ajoute un code postal à la table CP_ref si il n'est pas déjà présent
def ajouter_code_postal_cp_ref(code_postal, cur):
    try:
        cur.execute("""
            SELECT 1 FROM public."CP_ref" WHERE "Postal_Code" = %s;
        """, (code_postal,))
        result = cur.fetchone()

        if result:
            print(f"Le code postal {code_postal} existe déjà dans CP_ref, donc il n'est pas ajouté.")
        else:
            insert_query = """
            INSERT INTO public."CP_ref" ("Postal_Code")
            VALUES (%s);
            """
            cur.execute(insert_query, (code_postal,))
            print(f"Code postal {code_postal} ajouté à CP_ref avec succès.")
    except Exception as e:
        print(f"Erreur lors de l'ajout du code postal {code_postal} dans CP_ref : {e}")


def mettre_a_jour_geocodage_par_departement(department_codes, cur, get_cp_query, update_geocode_query, conn):
    mises_a_jour = []

    for department_code in department_codes:
        commune_code = get_commune_code(department_code)
        if commune_code:
            cur.execute(get_cp_query, (f"{department_code}%",))
            postal_codes = cur.fetchall()

            for postal_code_tuple in postal_codes:
                postal_code = postal_code_tuple[0]
                success = geocode_and_update_db(cur, commune_code, postal_code, conn)

                if success:
                    mises_a_jour.append((commune_code, postal_code))
                    print(f"Mise à jour réussie pour le code postal {postal_code}")
                else:
                    print(f"Échec de la mise à jour pour le code postal {postal_code}")

    conn.commit()
    print("Mises à jour des coordonnées géographiques terminées.")


#Met à jour les coordonnées géographiques pour les adresses non géocodées en utilisant CP_ref
def mettre_a_jour_coordonnees_geographiques(cur, conn):
    try:
        print(" Début de la mise à jour des coordonnées géographiques pour les adresses non géocodées...")

        # Sélectionner les lignes avec un code postal non géocodé
        cur.execute(get_cp_non_geocode_query)
        ListeClients = cur.fetchall()

        # Vérifier s'il y a des lignes à traiter
        if not ListeClients:
            print(" Toutes les adresses sont déjà géocodées. Rien à mettre à jour.")
            return

        for client in ListeClients:
            client_id = client[0]
            postal_code = client[1]

            # Rechercher les coordonnées géographiques dans CP_ref pour ce code postal
            cur.execute(get_coord_cp_ref_query, (postal_code,))
            geocode_data = cur.fetchone()

            if geocode_data:
                latitude, longitude, geometry = geocode_data

                # Vérification des valeurs avant mise à jour
                if latitude is None or longitude is None or geometry is None:
                    print(f" Données incomplètes pour le code postal {postal_code} (ID {client_id}) - Aucune mise à jour effectuée.")
                    continue

                if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
                    print(f"Coordonnées invalides pour le code postal {postal_code} (ID {client_id}): Latitude={latitude}, Longitude={longitude}")
                    continue

                # Convertir en JSON si nécessaire
                latitude_json = json.dumps(latitude)
                longitude_json = json.dumps(longitude)
                geometry_json = json.dumps(geometry)

                # Mettre à jour les coordonnées géographiques dans Adresses_Client
                cur.execute(update_cp_geocode_query, (latitude_json, longitude_json, geometry_json, client_id))
                print(f"Coordonnées mises à jour pour ID {client_id} - Lat: {latitude}, Lon: {longitude}")

            else:
                print(f"Aucune donnée trouvée pour le code postal {postal_code} (ID {client_id})")

        # Commit pour appliquer les changements
        conn.commit()
        print("Mise à jour des coordonnées géographiques terminée.")

    except Exception as e:
        print(f"Erreur lors de la mise à jour des coordonnées géographiques: {e}")
        conn.rollback()

#Gérer le dépassement du nombre de calls de l'API
def NbDeCall():
    global NbDeCallAPIGouv
    if NbDeCallAPIGouv <50 :
        NbDeCallAPIGouv += 1
    else :
        print("Limite atteinte, attente de 1 seconde...")
        time.sleep(1)
        NbDeCallAPIGouv = 0