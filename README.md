    #Géocodage des adresses et gestion des codes postaux :
  
Ce script permet de géocoder des adresses et de gérer les cas d'adresses incomplètes ou contenant uniquement des codes postaux. 
Mes données ston stockées sur un serveur PostgreSQL dans une table Adresses_Client.
Je vais aussi, à travers ce script créer une table CP_ref qui sera notre référentiel de codes postaux.
Utilisation de l'api : https://api-adresse.data.gouv.fr


Voici les principales étapes du processus :

    #Préparation des données :

- Filtrage des lignes sans ville ni code postal.
- Correction des codes postaux via la fonction corriger_code_postal.
- Construction des adresses complètes (rue, ville, code postal) avec la fonction concatener_adresse. 
- Nettoyage des adresses pour les rendre compatibles avec les API (suppression des accents, caractères spéciaux, etc.) via la fonction nettoyer_adresse.


    #Géocodage des adresses :

- Les adresses nettoyées sont géocodées pour obtenir leurs coordonnées géographiques (latitude, longitude) grâce à la fonction geocoder_adresse.
- Création et gestion de la table référentielle CP_ref :
Cette table contient tous les codes postaux français avec leurs coordonnées géographiques (latitude, longitude, geometry).
Les données sont extraites d'une source OpenData et mises à jour dynamiquement en cas de nouveaux codes postaux.
    
    
    #Gestion des cas particuliers :

- Adresse incomplète : Tenter un geocodage du code postal
- Code postal uniquement :
Recherche des coordonnées dans la table CP_ref.
Si absent, récupération des données via l'API OpenDataSoft, mise à jour de CP_ref et ajout des coordonnées correspondantes.


Il y a 3 fichiers : 
- Geocodage_PG.py : contient le script principal à éxecuter 
- Fonctions_Geocodage_PG.py : contient les définitions de toutes les fonctions
- Ressources_Geocodage.py : contient tous les éléments nécessaires à la connexion de données et aux requêtes sur PostgreSQL
