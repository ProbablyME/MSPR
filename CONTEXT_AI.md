Analyse Académique du Projet ETL – Évaluation de l'Empreinte Carbone du Réseau Ferroviaire Européen
1. Contexte et Objectif
Objectif du projet :
L'objectif central de ce projet est de concevoir et d'implémenter un pipeline d'intégration de données (ETL) capable de calculer et de centraliser les émissions de CO₂ par passager pour les trajets ferroviaires européens. Le système agrège des données ouvertes de transport public (GTFS) et des données spécifiques aux trains de nuit territoriaux (« Back on Track »).

Problématique technique et métier :
D'un point de vue métier, le projet s'inscrit dans un contexte Green IT, visant à fournir une visibilité granulaire sur l'impact écologique du transport ferroviaire européen segmenté par type de service (Grande Vitesse vs. Intercité). D'un point de vue technique, la problématique réside dans l'hétérogénéité et la fragmentation des sources de données de mobilité géographique. Il n'existe pas de référentiel unique continental des gares ferroviaires ; l'ETL doit donc relever le défi complexe d'unifier ces réseaux, de résoudre les homonymes et d'effectuer une réconciliation spatiale précise.

Contexte Big Data & Data Engineering :
L'ingestion de flux de transport à une échelle pan-européenne génère d'immenses volumétries de données (milliers de stations, millions d'arrêts et de trajectoires temporelles). Le choix d'Apache Spark (via PySpark) justifie directement cette nécessité de posséder un moteur de calcul distribué pour exécuter des calculs géospatiaux massifs, tandis que PostgreSQL répond au besoin de structuration relationnelle en aval (Data Warehouse).

2. Architecture Générale
Architecture globale du système :
Le système repose sur un paradigme de traitement par lots (Batch Processing). L'architecture est modulaire et se décompose en trois strates principales :

Couche d'Extraction (Sources) : Fichiers statiques externes téléchargés dynamiquement depuis des dépôts GTFS (définis par 
feeds_v2.csv
) et des fichiers tabulaires statiques locaux.
Couche de Traitement (Processing) : Apache Spark, instancié via 
etl_new_schema.py
, agissant comme moteur de calcul en mémoire pour nettoyer, transformer et enrichir les données.
Couche de Stockage (Data Mart) : Base de données relationnelle PostgreSQL modélisée sous la forme d'un schéma en étoile/flocon (schéma mart), optimisée pour la requêtage analytique et l'alimentation d'une application de visualisation Web (via Flask/Leaflet).
Organisation et responsabilités :

etl_new_schema.py
 : Cœur algorithmique gérant l'orchestration, la déduplication et l'intégration.
schema.sql
 : Définition DDL (Data Definition Language) de la cible analytique (table de faits, tables de dimensions).
visualisation.py
 : Consommateur final des données (Dashboarding cartographique).
3. Description Détaillée du Pipeline ETL
Phase d’Extraction
Les données proviennent d'archives .zip GTFS (ex: MobilityDatabase). Le pipeline tire parti des DataFrames Spark pour ingérer nativement les fichiers texte structurés (stops.txt, routes.txt, stop_times.txt).

Phase de Transformation
Cette phase concentre l'essentiel de la complexité métier et recourt à diverses facettes de l'API Spark :

Déduplication Spatiale (Clustering) : Regroupement de stations de transport distantes de moins de 150 mètres. Cette logique s'appuie sur la formule mathématique de Haversine croisée à un algorithme d'« Union-Find » permettant d'assigner un station_id canonique unique à des arrêts physiquement redondants provenant de flux GTFS distincts.
Fonctions de Fenêtrage (Window Functions) : L'utilisation particulièrement pertinente de la fonction lead() partitionnée par trip_id et ordonnée par stop_sequence pour construire des segments ou tronçons spatiaux continus (gare A vers gare B).
Logique Métier et Enrichissement : Application de jointures conditionnelles pour identifier la typologie de train (ex: détection du mot "TGV" ou "ICE" via expressions régulières) et leur assigner un facteur de calcul CO₂ variable selon qu'il s'agisse de Grande Vitesse ou d'Intercité.
Phase de Chargement
Les données normalisées sont écrites vers l'entrepôt PostgreSQL de manière itérative. L'injection s'appuie sur des commandes natives SQL via un module temporaire, couplées avec des clauses d'idempotence (INSERT ... ON CONFLICT DO NOTHING) afin d'assurer la robustesse du chargement sans risquer les violations de contraintes d'unicité (Primary/Foreign Keys).

4. Analyse Technique
Gestion des performances et du parallélisme :
L'implémentation de calculs intermédiaires (
haversine_col
, jointures) via les DataFrames Spark permet théoriquement une exécution répartie (lazy evaluation / DAG de Spark). L'usage de jointures larges générera mécaniquement des opérations de Shuffle sur le réseau, exigeant un bon partitionnement mémoire.

Gestion des erreurs, Qualité et Robustesse :
Le nettoyage de la donnée est rigoureux en amont (fonction 
is_valid_coord
 pour purger les aberrations géographiques lat/lon à zéro ou hors limites, normalisation des chaînes de caractères 
normalize_station_name
). La base PostgreSQL protège l'intégrité de ses données à l'aide de contraintes fortes (Foreign Keys, contraintes CHECK et UNIQUE).

5. Bonnes Pratiques et Limites
Évaluation des bonnes pratiques
Modélisation de la donnée : Le choix d'une architecture dimensionnelle (Tables dim_* et fact_emission) démontre une forte maturité et est parfaitement alignée sur l'état de l'art de la Business Intelligence (méthode de Kimball).
Idempotence : Le pipeline est ré-exécutable (rerunnable). En cas de crash, les clauses d'upsert garantissent que les données ne seront pas dupliquées.
Modularité du code : Code Python hautement structuré, lourdement commenté, avec une séparation stricte des domaines (téléchargement, déduplication spatiale, traitement des itinéraires).
Limites techniques majeures et Oaxes d'amélioration
Anti-pattern PySpark de Chargement (df.collect()) :
L'observation majeure du code (
write_to_postgres_upsert
 et 
write_to_postgres_simple
) révèle l'utilisation répétée de df.collect(). Cette instruction transfère l'ensemble des données des "Executors" vers le "Driver". Sur des volumétries massives, ceci invalide le principe de distribution de Spark et provoquera inévitablement une erreur de mémoire saturée (OutOfMemoryError). 💡 Amélioration : Utiliser l'API JDBC formelle de Spark (df.write.jdbc(..., mode="append")) permettant de paralléliser les écritures SQL en base directement depuis les différents workers du cluster.
Goulot d'étranglement algorithmique (Union-Find) :
L'algorithme de clustering des stations est exécuté en pur Python natif sur des dictionnaires rapatriés en mémoire. Une solution Big Data native aurait impliqué l'utilisation de GraphX/GraphFrames ou un index spatial (comme H3 de Uber) pour dédoublonner les entités de manière 100% distribuée.
6. Synthèse
Résumé et perspective de fonctionnement :
Le projet accomplit avec succès sa mission de consolider de multiples sources de trafic ferroviaire hétéroclites en une plateforme analytique centralisée avec son propre référentiel. Techniquement, le pipeline lit des configurations fragmentées, recalibre mathématiquement un planisphère réseau (pour annuler la complexité de dizaines de réseaux distincts) et traduit de simples "arrêts" en trajets valorisés d'un point de vue éco-responsable.

Conclusion académique :
Ce projet illustre remarquablement la dualité inhérente au Data Engineering moderne : résoudre une problématique physique et d'ingénierie (gestion d'une volumétrie hétéroclite asynchrone) dans le but d'en dégager une pure valeur sémantique (informer la décision écologique). Bien que l'architecture logicielle des Dataframes soit solidement construite, le franchissement du cap de la très large échelle (Big Data industriel) nécessitera la suppression des goulots d'étranglement du nœud Driver (Driver-side bottlenecks) identifiés lors de la phase de chargement et de clustering. L'ensemble n'en reste pas moins un projet de haut vol, très instructif quant aux défis du Big Data appliqué à l'IoT/Open Data.


