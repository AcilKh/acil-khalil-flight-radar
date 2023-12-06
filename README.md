# ACIL KHALIL - Kata FlightRadar24

## Technologies utilisées

Concernant l'extraction des données, j'ai utilisé Python. 
De plus la librairie https://github.com/JeanExtreme002/FlightRadarAPI facilite l'utilisation de l'API en python.
Enfin, c'est une technologie que j'ai utilisé en entreprise et à l'école. 
Je suis donc à l'aise avec cette dernière.

Concernant la transformation des données, j'ai utilisé le framework Apache Spark qui était demandée pour réaliser ce kata.
J'ai de plus utilisé Pyspark qui est une bibliothéque Python pour le développement de cette pipeline de donnée car elle possède une riche bibilothèque
d'une part.
D'autre part, et étant donné ma familiarité avec le language Python, cela m'a permis d'utiliser les bibliothèques Python existantes et de bénéficier 
de la simplicité et de la lisibilité du langage.

Enfin, en ce qui concerne la partie stockage des données, j'utilise le serveur de stockage d'objets open source Minio.
En effet, pour ce kata, je souhaitais héberger mes données dans un serveur de stockage accessible sur serveur distant ou en local.
De plus, je souhaitais trouver un serveur de stockage d'objet Open source et facile à utiliser et à déployer dans des environnements de conteneurs.
En faisant quelques recherches, je suis donc tombé sur Minio qui répondait à mes exigeances. 
Concernant le cheminement, j'utilise dans un premier temps Minio pour stocker mes données brutes dans la couche Bronze de ma bucket.
Ensuite, dans la partie data cleaning, je récupère mes données directement depuis Minio et effectue les transformations nécessaires.
Je stocke ensuite mes données dans la même bucket mais à un endroit différent correspondant à ma couche Silver.
Enfin, pour l'analyse des données, je récupère toujours à partir de Minio et depuis le répertoire Silver mes données. 
J'effectue différentes opérations (calculs, agrégation, calcul de rang,etc.) pour répondre aux questions du Kata puis
je stocke mes données préparées dans une nouvelle couche Gold dans Minio.
Ainsi, j'ai mes 3 couches Bronze, Silver et Gold permettant une gestion efficace des données.


## Orchestration des tâches

La pipeline de donnée est déclenchée grâce à l'orchestrateur de tâche Airflow. Airflow déclenche toutes les deux
heures l'extraction puis la transformation et enfin le chargement des données. 
Je nettoye et supprime également l'ensemble des conteneurs utilisées en fin de processus.

Pour lancer Airflow, j'utilise Docker ce qui nous facilite la création d'un environnement de test de manière rapide et
efficace.

## Format de données

Concernant le format de données, je stocke les données brutes provenant de l'API en format JSON. J'ai fait ce choix car c'est un format
particulièrement adapté aux données imbriquées. Dans notre cas, les données des vols sont justement très imbriquées.
De plus, c'est un format de  données lisible par l'homme et donc facilement compréhensible.
Enfin et concernant l'injestion des données dans la couche Silver et Gold,
j'utilise un format binaire tel que PARQUET.
C'est un format de stockage colonne qui offre une compression efficace et une utilisation optimale de l'espace de stockage.
Il est donc particulièrement adapté pour stocker des données transformées et nettoyées dans les couches Silver et Gold  où la performance et l'efficacité de stockage sont importantes.

## Data cleaning

L'API FlightRadar24 comporte de nombreux vols avec des informations manqantes. De ce fait, je n'ai pas directement récupérer les données de l'API pour les stocker dans 
ma couche Bronze. En effet, il y a eu un processus de data cleaning en amont. Cela m'a permis de récupérer les vols ayant des informations complètes.
Pour ce faire, j'ai notamment fait le lien avec les données de types détails également fourni par l'API.
Ces données donnent des informations sur la compagnie ou encore les durées de vols nécessaires pour répondre aux questions.


