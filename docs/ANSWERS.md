# Réponses du test

## Utilisation de la solution (étapes 1 à 3)

### Documentation Technique du Projet ETL

#### Introduction

Le projet a pour objet d'implémenter le processus ETL (Extraction, Transformation et Chargement) à partir des données de 3 API relatives aux utilisateurs, aux pistes et à l'historique d'écoute.

#### Architecture du Projet

##### Outils Utilisés
- Le projet utilise AirFlow comme outil d'orchestration de workflows
- Docker est utilisé pour conteneuriser l'environnement de développement
- Postgres est utilisé comme destination des données en raison de leur structure fixe et relationnelle

#### ETL - Extraction

##### Description
- L'étape d'extraction est réalisée à l'aide d'une interface appelée BaseTransformer et d'une implémentation appelée GenericTransformer
- GenericTransformer est responsable de l'ingestion des données de l'API
- En cas de surgissement d'un nouveau donné nécessitant une ingestion personnalisée, il suffit de créer une nouvelle classe qui implémente cette ingestion

#### ETL - Transformation

##### Description
- L'étape de transformation valide les données, telles que le genre et la date.
- Les doublons d'adresses e-mail pour le même utilisateur ne sont pas acceptés.
- Les validations sont implémentées dans l'interface pour être partagées entre les classes filles.
- Chaque classe fille doit implémenter la méthode transform en fonction des règles métier.

#### ETL - Chargement

##### Description
- Postgres est utilisé comme destination des données en raison de leur structure fixe et relationnelle.
- D'autres types de destinations auraient pu être utilisés, en fonction des besoins métier.

#### Exécution du Projet

##### Étapes
1. Pour exécuter le projet en local, il suffit d'avoir Docker installé et de lancer la commande docker-compose up --build dans le répertoire du projet.
2. Accédez à l'adresse localhost:8000 pour accéder à l'outil AirFlow.
3. Connectez-vous avec l'utilisateur airflow et le mot de passe airflow.
4. Accédez à la DAG music_etl et cliquez sur le bouton play pour l'exécuter.

#### Considérations

- Certaines choix auraient pu être différents dans la vie réelle, en tenant compte des outils utilisés par l'équipe, des discussions entre les membres de l'équipe et de la portée définie de la manière dont le projet de recommandation attend de recevoir les données.

#### Technologies Utilisées

- AirFlow
- Docker
- Postgres
- Python

## Questions (étapes 4 à 7)

### Étape 4

Pour stocker les informations issues des trois sources de données, j'ai recours à un système de base de données relationnelle, en raison de sa capacité à gérer des données structurées et relationnelles. Toutefois, dans le contexte d'un système d'intelligence artificielle, il convient également de prendre en compte des options de stockage de données plus spécialisées. Le choix du système de stockage de données sera fonction des besoins spécifiques du système d'intelligence artificielle et du volume de données à traiter. En outre, il est essentiel de considérer l'intégration avec d'autres technologies d'intelligence artificielle, notamment les frameworks d'apprentissage automatique et les bibliothèques de traitement du langage naturel.

### Étape 5

Pour assurer la surveillance du pipeline de données, j'ai mis en place les éléments suivants :

#### Méthode de surveillance
- J'utilise Airflow pour surveiller l'exécution quotidienne du pipeline
- J'ai journalisé les étapes de l'ETL pour suivre les performances
- Cela me permet d'identifier rapidement les problèmes éventuels

#### Métriques clés
- Taux de réussite de l'exécution du pipeline
- Temps d'exécution moyen du pipeline
- Nombre d'erreurs et d'exceptions
- Volume de données traitées

### Étape 6

Pour automatiser le calcul des recommandations, je suivrais les étapes suivantes :

1. Récupération des données : Je récupérerais les données stockées par l'ETL décrite précédemment.

2. Suppression des colonnes inutiles : Je supprimerais les colonnes inutiles, telles que le nom et l'adresse e-mail.

3. Chargement des données dans Pandas : Je chargerais les données dans Pandas.

4. Modifications pour améliorer les performances : Je effectuerais les modifications nécessaires pour améliorer les performances de l'algorithme, notamment :

- La normalisation des colonnes quantitatives.
- Le codage des colonnes descriptives (Label Encoder pour le genre musical et Ordinal Encoder pour le sexe).

Après cette étape, je séparerais les données en ensembles d'entraînement et de test pour évaluer les performances du modèle.

Ensuite, j'utiliserais les algorithmes de filtrage collaboratif et mesurerais les performances du modèle avec les métriques de précision (Précision, Rappel, Exactitude).

Si le modèle n'atteint pas le seuil de performance défini par le client, je reviendrais aux étapes précédentes pour :

- Réviser les hyperparamètres du modèle.
- Ajuster le prétraitement des données.
- Considérer des techniques de régularisation ou de sélection de caractéristiques.

Après ajustements, je réévaluerais les performances du modèle et répéterais le processus jusqu'à atteindre les résultats souhaités.

### Étape 7

Pour automatiser le réentrainement du modèle de recommandation, je suivrais les étapes suivantes :

1. Planification : Je planifierais la fréquence de réentrainement en fonction des besoins du système et des changements dans les données.

2. Récupération des données : Je récupérerais les nouvelles données pour réentrainer le modèle.

3. Prétraitement des données : Je réappliquerais les mêmes étapes de prétraitement utilisées initialement (normalisation, encodage, etc.).

4. Réentrainement : Je réentrainerais le modèle en utilisant les nouvelles données et les mêmes algorithmes de filtrage collaboratif.

5. Évaluation : Je réévaluerais les performances du modèle réentrainé en utilisant les mêmes métriques de précision (Précision, Rappel, Exactitude).

6. Détection de surapprentissage (Overfitting) : Je vérifierais la présence de surapprentissage en utilisant des techniques telles que :
    - Validation croisée
    - Évaluation sur un jeu de données de test

7. Correction du surapprentissage : Si nécessaire, je ajusterais les hyperparamètres du modèle, utiliserait des techniques de régularisation ou réduirait la complexité du modèle.

8. Deployment : Je déployerais le modèle réentrainé dans le système.

Pour automatiser ce processus, je utiliserais des outils tels que :

- Apache Airflow pour planifier et orchestrer les tâches.
- Docker pour conteneuriser le modèle et les dépendances.
- Git pour versionner le code et les données.
- Cloud services (AWS, Google Cloud, Azure) pour héberger et exécuter le modèle.
