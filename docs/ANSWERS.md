# Réponses du test

## Utilisation de la solution (étapes 1 à 3)

### Documentation Technique du Projet ETL

#### Introduction

L'objectif du projet est de mettre en place un processus ETL (Extraction, Transformation et Chargement) qui permettra d'obtenir des données à partir d'une application musicale en lisant 3 endpoints concernant les utilisateurs, les chansons disponibles et l'historique d'écoute.

#### Architecture du Projet

##### Outils Utilisés
- Le projet utilise AirFlow comme outil d'orchestration de workflows
- Docker est utilisé pour conteneuriser l'environnement de développement
- Postgres est utilisé comme destination des données en raison de leur structure fixe et relationnelle

#### ETL - Extraction

##### Description
- L'étape d'extraction est réalisée à l'aide d'une interface appelée BaseExtractor et d'une implémentation appelée GenericExtractor
- GenericExtractor est responsable de l'ingestion des données de l'API 
- En cas de surgissement d'un nouveau donné nécessitant une ingestion personnalisée, il suffit de créer une nouvelle classe qui implémente cette ingestion

#### ETL - Transformation

##### Description
L'étape de transformation valide les données, telles que le genre et la date, en utilisant la bibliothèque Pandas pour un traitement efficace et flexible des données. Les avantages de l'utilisation de Pandas incluent :

- Traitement rapide et efficace des grands ensembles de données
- Fonctionnalités avancées de manipulation de données, telles que le regroupement, le tri et la fusion de données
- Intégration facile avec d'autres bibliothèques Python pour la science des données

Les doublons d'adresses e-mail pour le même utilisateur ne sont pas acceptés. Les validations sont implémentées dans l'interface BaseTransformer pour être partagées entre les classes filles. Chaque classe fille doit implémenter la méthode transform en fonction des règles d'affaires.

#### ETL - Chargement

##### Description
La phase de chargement des données a été effectuée en exploitant l'interface BasePostgresLoader, avec des implémentations spécifiques dans les classes:
 - GenericPostgresLoader pour les données utilisateurs et musiques
 - ListenHistoryPostgresLoader pour les données d'historique d'écoute, qui intègre un traitement particulier pour vérifier l'insertion préalable des données associées aux utilisateurs et chansons.

Postgres a été choisi comme destination des données en raison de leur structure fixe et relationnelle. D'autres types de destinations auraient pu être utilisés, en fonction des besoins métier.

Pour visualiser les données stockées, suivez les étapes ci-dessous :

1. Accédez à l'interface d'administration de la base de données (PGAdmin) via l'adresse : http://localhost:5050.
2. Entrez les informations d'identification :
    - Utilisateur : admin@admin.com
    - Mot de passe : admin
3. Connectez-vous à la base de données "music" en utilisant les informations suivantes :
    - Hôte : postgres
    - Utilisateur : airflow
    - Mot de passe : airflow

#### Exécution du Projet

##### Étapes
1. Pour exécuter le projet en local, il suffit d'avoir Docker installé et de lancer la commande docker-compose up --build dans le répertoire du projet.
2. Accédez à l'adresse localhost:8000 pour accéder à l'outil AirFlow.
3. Connectez-vous avec l'utilisateur airflow et le mot de passe airflow.
4. Accédez à la DAG music_etl et cliquez sur le bouton play pour l'exécuter.

#### Considérations

- Certaines choix auraient pu être différents dans la vie réelle, en tenant compte des outils utilisés par l'équipe, des discussions entre les membres de l'équipe et de la portée définie de la manière dont le projet de recommandation attend de recevoir les données.
- Des termes anglais ont été utilisés dans les méthodes et les variables pour maintenir la cohérence avec le code existant.
- Pour accéder aux données brutes, veuillez consulter l'API à l'adresse suivante : http://localhost:8000

#### Technologies Utilisées

- AirFlow
- Docker
- Postgres
- Python

## Questions (étapes 4 à 7)

### Étape 4

Pour stocker les informations issues des trois sources de données, j'ai conçu un schéma de base de données relationnelle en utilisant **PostgreSQL**. Ce système a été choisi pour sa capacité à gérer efficacement des données structurées, sa robustesse, et son extensibilité. Voici les détails du schéma et des raisons de ce choix.

#### Schéma de la base de données
1. **Table `tracks`** : Stocke les informations relatives aux chansons, y compris leurs caractéristiques principales comme le titre, l’artiste, les auteurs-compositeurs, les genres, et la durée. Les colonnes incluent :
   - `id` (clé primaire) : Identifiant unique pour chaque morceau.
   - `name`, `artist`, `album` : Informations textuelles sur le changson.
   - `songwriters` et `genres` : Colonnes pour les données textuelles ou multiples.
   - `created_at` et `updated_at` : Permettent de suivre les modifications.

2. **Table `users`** : Stocke les informations sur les utilisateurs, y compris leurs préférences musicales et leurs identifiants personnels. Les colonnes incluent :
   - `id` (clé primaire) : Identifiant unique pour chaque utilisateur.
   - `email` : Identifiant unique, indexé pour des recherches rapides.
   - `favorite_genres` : Pour capturer les genres préférés des utilisateurs.

3. **Table `listen_history`** : Enregistre les interactions des utilisateurs avec les chansons, reliant `users` et `tracks` via des clés étrangères. Les colonnes incluent :
   - `id` (clé primaire) : Identifiant unique pour chaque historique d’écoute.
   - `user_id` et `track_id` : Clés étrangères vers les tables `users` et `tracks`.
   - `created_at` : Date et heure de l’écoute.

#### Choix du système de base de données
- **PostgreSQL** a été choisi car il offre :
  - Une gestion efficace des relations entre les tables grâce aux clés étrangères.
  - Des fonctionnalités avancées comme le support des données semi-structurées avec JSONB, ce qui serait utile si certaines données provenant des sources sont semi-structurées.
  - Une forte intégration avec les frameworks ETL et les outils analytiques.
  - Une compatibilité avec les frameworks d’intelligence artificielle pour l’extraction de données nécessaires aux modèles.

#### Intégration avec l’ETL et l’intelligence artificielle
- Les données issues des trois sources sont transformées et nettoyées dans un pipeline ETL écrit en Python avant d’être insérées dans cette base.
- Pour des besoins analytiques ou d’apprentissage automatique, les données stockées dans PostgreSQL peuvent être directement exploitées grâce à des bibliothèques comme **SQLAlchemy** ou **Pandas**.

#### Extensions possibles
- En cas de besoins évolutifs ou d’un volume de données élevé, un entrepôt de données pourrait être envisagé. Ces solutions sont mieux adaptées pour des analyses complexes sur de grandes quantités de données.

### Étape 5

Pour assurer la surveillance du pipeline de données, j'ai mis en place les éléments suivants :

#### Méthode de surveillance
- **Utilisation d'Airflow :** 
  - J'ai configuré Airflow pour orchestrer et surveiller l'exécution quotidienne du pipeline. 
  - L'interface d'Airflow permet de visualiser en temps réel l'état des tâches (succès, échec ou en cours) et de relancer manuellement les tâches échouées si nécessaire.
  - Des logs détaillés sont accessibles directement dans l'interface pour chaque étape du pipeline, facilitant le diagnostic des problèmes.

- **Journalisation avancée :** 
  - J'ai mis en place un système de journalisation détaillé à chaque étape du pipeline ETL (Extraction, Transformation, Chargement).
  - Ces logs sont intégrés dans Airflow pour offrir une visibilité complète sur les performances et les anomalies.

#### Métriques clés
- **Taux de réussite :** Pourcentage des exécutions du pipeline complétées avec succès.
- **Temps d'exécution moyen :** Temps moyen nécessaire pour compléter une exécution du pipeline, disponible dans l'interface d'Airflow.
- **Nombre d'erreurs et d'exceptions :** Compte des échecs ou exceptions visibles directement dans les logs d'Airflow.
- **Volume de données traitées :** Quantité de données (enregistrements ou taille) extraites, transformées et chargées, surveillée via les étapes définies dans Airflow.



### Étape 6


Pour automatiser le calcul des recommandations, je suivrais les étapes suivantes :

#### Étapes détaillées

1. **Récupération des données :**
   - Les données ingérées quotidiennement par le pipeline ETL seraient extraites depuis le stockage (par exemple, une base de données relationnelle ou un data lake).

2. **Prétraitement des données :**
   - **Nettoyage des données :** Suppression des colonnes inutiles telles que le nom et l'adresse e-mail, qui n'ont pas d'impact sur les recommandations.
   - **Normalisation :** Application de la normalisation aux colonnes quantitatives pour standardiser les valeurs.
   - **Encodage des catégories :** Utilisation de :
     - **Label Encoding** pour des colonnes comme les genres musicaux.
     - **Ordinal Encoding** pour des colonnes ordinales comme le sexe.

3. **Chargement des données dans un environnement de traitement :**
   - Les données sont chargées dans un environnement comme **Pandas** ou directement dans un pipeline compatible avec des frameworks de machine learning tels que **TensorFlow**, **PyTorch** ou **Scikit-learn**.

4. **Construction et exécution du modèle de recommandation :**
   - Utilisation d'algorithmes de **filtrage collaboratif** (basé sur les utilisateurs ou les éléments) ou de **modèles hybrides** (ajoutant des données de contenu comme les genres ou l'historique d'écoute).
   - Séparation des données en ensembles d'entraînement et de test pour évaluer les performances du modèle.

5. **Évaluation du modèle :**
   - Mesures des performances du modèle à l'aide de métriques telles que :
     - **Précision**
     - **Rappel**
     - **Exactitude**
   - Comparaison des résultats aux seuils définis par le client.

6. **Boucle d’amélioration continue :**
   - Si les résultats sont en deçà des attentes :
     - Ajustement des hyperparamètres via une recherche systématique (GridSearch, RandomSearch, ou Optuna).
     - Modification du prétraitement des données pour mieux refléter les relations sous-jacentes.
     - Introduction de techniques de régularisation ou de réduction de la dimensionnalité.

#### Automatisation du processus
- Le processus complet serait orchestré dans un outil, permettant d'exécuter le calcul des recommandations sur une base régulière.
- Les modèles seraient sauvegardés et versionnés dans un gestionnaire comme **MLflow** pour garantir la traçabilité.
- Les résultats des recommandations seraient stockés dans une base de données ou une API REST pour être facilement accessibles par les systèmes clients.


### Étape 7

Pour automatiser le réentrainement du modèle de recommandation, je suivrais les étapes suivantes :

#### Étapes du processus
1. **Planification :**
   - Définir la fréquence de réentrainement en fonction des besoins du système (quotidien, hebdomadaire, ou basé sur un seuil de changement dans les données).

2. **Récupération des données :**
   - Extraire les nouvelles données collectées par le pipeline ETL pour refléter les comportements récents des utilisateurs.

3. **Prétraitement des données :**
   - Réappliquer les mêmes étapes de prétraitement utilisées initialement, notamment :
     - **Normalisation** des colonnes quantitatives.
     - **Encodage** des catégories (Label Encoding, Ordinal Encoding).

4. **Réentrainement :**
   - Utiliser les nouvelles données pour réentrainer le modèle avec les mêmes algorithmes de filtrage collaboratif ou hybrides.
   - Optionnellement, inclure des techniques comme **transfer learning** si applicable.

5. **Évaluation :**
   - Mesurer les performances du modèle réentrainé à l’aide des mêmes métriques :
     - **Précision**
     - **Rappel**
     - **Exactitude**
   - Comparer les résultats au modèle précédent pour vérifier les améliorations ou éviter les régressions.

6. **Détection de surapprentissage (Overfitting) :**
   - Effectuer une validation croisée pour identifier les risques de surapprentissage.
   - Évaluer le modèle sur un jeu de données de test indépendant.

7. **Correction du surapprentissage :**
   - Si nécessaire :
     - Ajuster les hyperparamètres du modèle.
     - Appliquer des techniques de régularisation (L1, L2, Dropout).
     - Réduire la complexité du modèle en sélectionnant des caractéristiques pertinentes.

8. **Déploiement :**
   - Déployer le modèle réentrainé dans le système, en remplaçant l’ancien modèle ou en maintenant une version parallèle pour tests (blue/green deployment).

#### Automatisation du processus
Pour automatiser ce workflow, j'utiliserais les outils suivants :
- **Apache Airflow :** Planification et orchestration des tâches de réentrainement.
- **Docker :** Conteneurisation du modèle et de ses dépendances pour assurer la portabilité.
- **Git :** Versionnement du code et des configurations associées au modèle.
- **Cloud services (AWS, Google Cloud, Azure) :** Hébergement des données, exécution du réentrainement, et déploiement du modèle.



