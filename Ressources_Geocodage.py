# Ressources_Geocodage.py
import sys
import psycopg2

# Assignation des arguments
postgres_user = sys.argv[1]
postgres_password = sys.argv[2]

# URL de connexion
URL_SERVER_POSTGRE = "VOTRE SERVEUR"
DATABASE_POSTGRE = "VOTRE DB"
NbDeCallAPIGouv = 0


# Connexion à la base de données
def connect_to_postgres():
    """Établit une connexion avec la base de données PostgreSQL."""
    try:
        connection = psycopg2.connect(
            host=URL_SERVER_POSTGRE,
            database=DATABASE_POSTGRE,
            user=postgres_user,
            password=postgres_password
        )
        return connection
    except Exception as e:
        print(f"Erreur de connexion à la base de données : {e}")
        raise


# Permet de mettre Is_Geocoded=True et Is_Error=True pour ne pas retraiter les lignes sans ville ni cp
update_filtre_query = """
    UPDATE public."Adresses_Client"
    SET "Is_Geocoded" = TRUE
    AND "Is_Error" =TRUE
    WHERE "PersonMailingAddress.city" IS NULL
      AND "PersonMailingAddress.postalCode" IS NULL
      AND "Is_Geocoded" = FALSE
      OR "PersonMailingAddress.postalCode" SIMILAR TO '^\\d{2,5}$';
    """

# Permet de sélectionner les lignes avec CP non null
select_cp_query = """
        SELECT "PHX_NumeroICU__c", "PersonMailingAddress.postalCode" 
        FROM public."Adresses_Client" 
        WHERE "PersonMailingAddress.postalCode" IS NOT NULL;
    """

select_cp_non_corrige_query = """
        SELECT "PHX_NumeroICU__c", "PersonMailingAddress.postalCode" 
        FROM public."Adresses_Client" 
        WHERE "Is_Geocoded" = FALSE"""

# Permet de mettre à jour les codes postaux sous le format nettoyé dans la table Adresses_Client
update_query = """
    UPDATE public."Adresses_Client"
    SET "PersonMailingAddress.postalCode" = %s
    WHERE "PHX_NumeroICU__c" = %s;
    """

# Permet de sélectionner les lignes qui ont une ville / un cp / un nom de rue
selection_lignes_query = """
        SELECT "PHX_NumeroICU__c", "PersonMailingAddress.street", "PersonMailingAddress.city", "PersonMailingAddress.postalCode" 
        FROM public."Adresses_Client" 
        WHERE "PersonMailingAddress.city" IS NOT NULL 
        AND "PersonMailingAddress.street" IS NOT NULL
        AND "PersonMailingAddress.postalCode" IS NOT NULL;
    """

update_address_query = """
    UPDATE public."Adresses_Client"
    SET "Adresse_complete" = %s
    WHERE "PHX_NumeroICU__c" = %s;
    """

update_geocode_query = """
    UPDATE public."Adresses_Client"
            SET "Latitude" = %s, 
                "Longitude" = %s,
                "Geometry" = %s,
                "Is_Geocoded" = TRUE,
                "Is_Adresse_Complete" = %s
            WHERE "PHX_NumeroICU__c" = %s;
    """

get_cp_query = """
                SELECT "Postal_Code"
                FROM public."CP_ref"
                WHERE "Postal_Code" LIKE %s AND "Is_Error" = TRUE
            """

get_cp_non_geocode_query = """
        SELECT ac."PHX_NumeroICU__c", ac."PersonMailingAddress.postalCode"
        FROM public."Adresses_Client" ac
        WHERE ac."PersonMailingAddress.postalCode" IS NOT NULL
        AND ac."Is_Adresse_Complete" =FALSE
          AND ac."Is_Geocoded" = FALSE;
    """

update_cp_geocode_query = """
    UPDATE public."Adresses_Client"
    SET "Latitude" = %s, 
        "Longitude" = %s,
        "Geometry" = %s,
        "Is_Geocoded" = TRUE
    WHERE "PHX_NumeroICU__c" = %s;
    """

get_coord_cp_ref_query = """
            SELECT "Latitude", "Longitude", "Geometry"
            FROM public."CP_ref"
            WHERE "Postal_Code" = %s AND "Is_Error"=FALSE;
        """

add_cp_query = """
        SELECT DISTINCT "PersonMailingAddress.postalCode" 
        FROM public."Adresses_Client"
        WHERE "PersonMailingAddress.postalCode" IS NOT NULL
        AND "Is_Geocoded" = FALSE AND "Is_Error"=FALSE;
    """

cp_non_digit_query = """UPDATE public."Adresses_Client"
SET "Is_Geocoded" = TRUE,
    "Is_Error" = TRUE
WHERE "PersonMailingAddress.postalCode" ~ '[^0-9]';"""