# Geocodage_PG.py
import sys

if len(sys.argv) != 3:
    print("Erreur : Veuillez fournir exactement 2 arguments (utilisateur et mot de passe PostgreSQL).")
    sys.exit(1)


import json
from Fonctions_Geocodage_PG import corriger_code_postal, corriger_codes_postaux, concatener_adresse, nettoyer_code_postal, nettoyer_adresse, geocoder_adresse, geocoder_codes_postaux, geocode_and_update_db, mettre_a_jour_adresses, mettre_a_jour_geocodage, mettre_a_jour_geocodage_par_departement, mettre_a_jour_coordonnees_geographiques, get_commune_code, get_department_code, ajouter_code_postal_cp_ref
from Ressources_Geocodage import cp_non_digit_query, connect_to_postgres, update_query, update_filtre_query, update_geocode_query, update_cp_geocode_query, update_address_query, select_cp_query, selection_lignes_query, get_cp_query, get_cp_non_geocode_query, get_coord_cp_ref_query, add_cp_query
import sys

def main():

    # Connexion à la base de données PostgreSQL
    conn = connect_to_postgres()
    print("Connexion à PostgreSQL réussie")

    # Création d'un curseur pour interagir avec la base de données
    cur = conn.cursor()

    # 0. Mettre à jour les codes postaux non numériques
    print("Mise à jour des adresses avec un code postal non numérique...")
    cur.execute(cp_non_digit_query)
    conn.commit()
    print("Codes postaux non numériques marqués comme erreur.")


    # 1. Flag des lignes sans ville ni code postal
    cur.execute(update_filtre_query)
    conn.commit()
    print("Lignes sans ville ni code postal répérées : Is_Error = True.")

    # 2. Mise à jour des codes postaux dans la table Adresses_Client
    print("Mise à jour des codes postaux dans Adresses_Client...")
    cur.execute(select_cp_query)
    ListeClients = cur.fetchall()

    corriger_codes_postaux(cur, update_query, conn)

    # 3. Création de l'adresse complète ligne par ligne
    print("Création des adresses complètes ligne par ligne...")

    # Sélection des lignes avec les champs nécessaires
    cur.execute(selection_lignes_query)
    ListeClients = cur.fetchall()

    mettre_a_jour_adresses(ListeClients, cur, update_address_query, conn)

    # 4. Vérification - pour l'ajout des codes postaux à CP_ref
    print("Ajout des codes postaux à la table CP_ref...")
    cur.execute(add_cp_query)
    codes_postaux = cur.fetchall()

    # 5. Ajout des codes postaux à CP_ref
    for code_postal in codes_postaux:
        ajouter_code_postal_cp_ref(code_postal[0], cur)
    conn.commit()
    print("Codes postaux ajoutés à CP_ref et table mise à jour.")

    # 6. Mise à jour des coordonnées géographiques dans la table Adresses_Client
    print("Mise à jour des coordonnées géographiques dans Adresses_Client...")

    # Mise à jour des lignes géocodées dans Adresse_Client
    mettre_a_jour_geocodage(ListeClients, cur, update_geocode_query, conn)

    # 7. Géocodage des codes postaux
    print("Géocodage des codes postaux et mise à jour des coordonnées géographiques dans CP_ref...")
    geocoder_codes_postaux(cur, conn)  # Appel de la fonction pour géocoder les codes postaux

    # 8. Géocodage des codes postaux en erreur :
            # Récupérer les codes de département et géocoder pour chaque code postal
    department_codes = get_department_code(cur)

    mettre_a_jour_geocodage_par_departement(department_codes, cur, get_cp_query, update_geocode_query, conn)

    # 9. Mise à jour des coordonnées géographiques pour les adresses non géocodées
    mettre_a_jour_coordonnees_geographiques(cur, conn)

    # Fermer le curseur et la connexion
    cur.close()
    conn.close()
    print("Connexion à la base de données fermée.")

if __name__ == "__main__":
    main()