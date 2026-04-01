from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    # 1. Session Spark
    spark = SparkSession.builder \
        .appName("CrimeProjectSpark") \
        .getOrCreate()

    # 2. Lecture du CSV brut (même dossier que ce script)
    df = spark.read.csv(
        "Crime_Data_from_2020_to_Present.csv",
        header=True,
        inferSchema=True
    )

    # ---------------- 3. PRÉPARATION / NETTOYAGE ----------------

    # 3.1 Réduction de colonnes (adaptée à ton notebook)
    colonnes_a_garder = [
        "DATE OCC", "TIME OCC", "AREA NAME", "Rpt Dist No",
        "Crm Cd", "Crm Cd Desc",
        "Vict Age", "Vict Sex", "Vict Descent",
        "Premis Cd", "Premis Desc",
        "Weapon Used Cd", "Weapon Desc",
        "Status", "LAT", "LON"
    ]
    df = df.select([c for c in colonnes_a_garder])

    # 3.2 Noms de colonnes en snake_case
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower().strip().replace(" ", "_"))

    # 3.3 Création du timestamp_occ (date + heure) - VERSION STABLE

    # a) convertir la date brute (format d'origine du dataset LA : MM/dd/yyyy)
    df = df.withColumn(
        "date_occ_ts",
        F.to_date("date_occ", "MM/dd/yyyy")
    )

    # b) convertir time_occ en chaîne HHmm sur 4 caractères
    df = df.withColumn(
        "time_occ_str",
        F.lpad(F.col("time_occ").cast("string"), 4, "0")
    )

    # c) extraire HH et mm
    df = df.withColumn("heure_str", F.col("time_occ_str").substr(1, 2))
    df = df.withColumn("minute_str", F.col("time_occ_str").substr(3, 2))

    # d) construire une colonne string "yyyy-MM-dd HH:mm:00"
    df = df.withColumn(
        "timestamp_str",
        F.concat_ws(
            " ",
            F.date_format("date_occ_ts", "yyyy-MM-dd"),
            F.concat_ws(":", F.col("heure_str"), F.col("minute_str"), F.lit("00"))
        )
    )

    # e) convertir en vrai timestamp
    df = df.withColumn(
        "timestamp_occ",
        F.to_timestamp("timestamp_str", "yyyy-MM-dd HH:mm:ss")
    )

    # f) nettoyer les colonnes intermédiaires
    df = df.drop("time_occ_str", "heure_str", "minute_str", "timestamp_str")

    # 3.4 Caractéristiques temporelles
    df = df.withColumn("heure", F.hour("timestamp_occ").cast("int"))
    df = df.withColumn("jour_semaine", F.date_format("timestamp_occ", "EEEE"))

    # 3.5 Nettoyage / imputations simples

    # Nettoyage des âges aberrants
    df = df.withColumn(
        "vict_age",
        F.when((F.col("vict_age") < 0) | (F.col("vict_age") > 100), None)
         .otherwise(F.col("vict_age"))
    )

    # Médiane approx. pour vict_age
    median_age = df.approxQuantile("vict_age", [0.5], 0.01)[0]
    df = df.fillna({"vict_age": int(median_age)})

    # Variables catégorielles
    df = df.fillna({
        "vict_sex": "Unknown",
        "vict_descent": "Unknown",
        "weapon_desc": "No weapon",
        "premis_desc": "NO DESC",
        "status": "Unknown"
    })

    # ---------------- 4. TRANSFORMATIONS (section 3.3) ----------------

    # 4.1 FILTER : garder crimes récents (2022+)
    df_filtre = df.filter(F.year("timestamp_occ") >= 2022)

    # 4.2 GROUPBY : nombre de crimes par zone
    crimes_par_zone = df_filtre.groupBy("area_name").count()

    # 4.3 GROUPBY : nombre de crimes par jour de semaine
    crimes_par_jour = df_filtre.groupBy("jour_semaine").count()

    # 4.4 MAP + REDUCEBYKEY sur RDD (crimes par jour de semaine)
    rdd_jour = df_filtre.select("jour_semaine").rdd.map(
        lambda row: (row["jour_semaine"], 1)
    )
    rdd_jour_counts = rdd_jour.reduceByKey(lambda a, b: a + b)

    # 4.5 JOIN avec petite table de référence des zones
    zones_ref = spark.createDataFrame(
        [
            ("Hollywood", "Central"),
            ("Van Nuys", "Valley"),
            ("Wilshire", "Central"),
            ("Pacific", "West"),
        ],
        ["area_name", "region"]
    )
    crimes_par_zone_region = crimes_par_zone.join(
        zones_ref, on="area_name", how="left"
    )

    # ---------------- 5. ACTIONS (section 3.3) ----------------

    # ACTION 1 : count
    nb_lignes_filtrees = df_filtre.count()
    print("Nombre de lignes après filtre :", nb_lignes_filtrees)

    # ACTION 2 : show
    crimes_par_zone.orderBy(F.col("count").desc()).show(10)

    # ACTION 3 : take sur le RDD
    top_jours = rdd_jour_counts.take(7)
    print("Crimes par jour (RDD) :", top_jours)

    # ACTION 4 : écriture d’un résultat agrégé (équiv. saveAsTextFile)
    crimes_par_zone_region.write.mode("overwrite").csv(
        "output_crimes_par_zone_region"
    )

    spark.stop()


if __name__ == "__main__":
    main()
