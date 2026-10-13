"""Mesure des deux méthodes Spark (RDD et DataFrame) sous Linux, dans le conteneur apache/spark.

Mêmes fonctions et même protocole que dans le notebook (sections 9 et 11).
Usage : spark-submit comptage_spark.py <coeurs> <fichier>:<repetitions> [...]
Affiche une ligne JSON par mesure.
"""
import json
import statistics
import sys
import time
from operator import add
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

coeurs = sys.argv[1]
t0 = time.perf_counter()
spark = (SparkSession.builder
         .master(f"local[{coeurs}]")
         .appName("comptage-mots")
         .config("spark.driver.memory", "4g")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
demarrage = time.perf_counter() - t0


def methode4_rdd(fichier):
    lignes = sc.textFile(fichier, minPartitions=sc.defaultParallelism)
    return (lignes.flatMap(lambda ligne: ligne.split())
                  .map(lambda mot: (mot, 1))
                  .reduceByKey(add)
                  .collectAsMap())


def methode4_df(fichier):
    requete = (spark.read.text(fichier)
                    .select(F.explode(F.split(F.trim("value"), r"\s+")).alias("mot"))
                    .where(F.col("mot") != "")
                    .groupBy("mot").count())
    return {ligne["mot"]: ligne["count"] for ligne in requete.collect()}


def chrono(fonction, fichier, repetitions):
    temps = []
    for _ in range(repetitions):
        t = time.perf_counter()
        fonction(fichier)
        temps.append(time.perf_counter() - t)
    return temps


print(json.dumps({"démarrage (s)": demarrage, "parallélisme": sc.defaultParallelism}), flush=True)
fichiers = [arg.rsplit(":", 1) for arg in sys.argv[2:]]
methode4_rdd(fichiers[0][0]); methode4_df(fichiers[0][0])   # échauffement
for fichier, rep in fichiers:
    for methode, f in [("4a. Spark RDD", methode4_rdd), ("4b. Spark DataFrame", methode4_df)]:
        temps = chrono(f, fichier, int(rep))
        print(json.dumps({"fichier": Path(fichier).stem, "cœurs": coeurs, "méthode": methode,
                          "temps (s)": statistics.median(temps), "min (s)": min(temps),
                          "max (s)": max(temps)}), flush=True)
spark.stop()
