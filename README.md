# Comptage de mots : Python simple, pseudo-MapReduce, Hadoop et Spark

Notebook : [`comptage_mots.ipynb`](comptage_mots.ipynb)

Comparaison expérimentale de méthodes de comptage d'occurrences de mots sur trois fichiers
texte (un mot par ligne) de 1 000, 100 000 et 10 000 000 lignes :

- **Partie I** : les trois méthodes du rapport *Comptage de mots* (Python simple,
  pseudo-MapReduce Python, Hadoop MapReduce) ;
- **Partie II** : Apache Spark via PySpark (RDD et DataFrame), y compris l'effet du nombre de cœurs.

## Reproduire

1. Démarrer le mini-cluster Hadoop 3.4.1 (HDFS + YARN, pseudo-distribué) :
   ```bash
   docker compose -f hadoop/docker-compose.yml up -d
   ```
2. Installer un JDK 17, 21 ou 25 (pour Spark) et les dépendances Python :
   `pip install jupyter pandas matplotlib pyspark`
3. Exécuter `comptage_mots.ipynb` depuis la racine du dépôt.
   Les fichiers de test sont générés dans `data/` (graine fixe, donc reproductibles).

Arrêt du cluster : `docker compose -f hadoop/docker-compose.yml down -v`
