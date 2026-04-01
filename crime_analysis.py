# Installer PySpark si nécessaire
!pip install pyspark

# =============================
# 0. Créer la SparkSession
# =============================
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("CrimeAnalysis") \
    .getOrCreate()

# =============================
# 1. Charger le dataset
# =============================
from google.colab import files
files.upload()  # ACTION 0 : Télécharger le fichier nettoyé depuis l'ordinateur local

df_spark = spark.read.csv("crime_clean_pandas.csv", header=True, inferSchema=True)
df_spark.show(5)       # ACTION 1 : Afficher les 5 premières lignes
df_spark.printSchema() # ACTION 2 : Vérifier le schéma du DataFrame

# =============================
# Transformation 1 : Filtrage des crimes avec âge valide
# =============================
from pyspark.sql.functions import col

df_vict_age = df_spark.filter(
    (col("vict_age").isNotNull()) & (col("vict_age") > 0)
)  # Transformation 1 : garder uniquement les lignes avec vict_age valide (>0)

# Actions suite à Transformation 1
df_vict_age.show(10)  # ACTION 3 : Afficher 10 lignes filtrées
df_vict_age.count()   # ACTION 4 : Nombre total de crimes avec âge valide

# =============================
# Transformation 1b : Analyse par groupe d'âge
# =============================
df_crimes_par_age_group = (
    df_vict_age
        .groupBy("vict_age_group")  # Transformation 1b : regrouper par groupe d'âge
        .count()                    # compter le nombre de crimes par groupe
        .orderBy("count", ascending=False)  # trier du groupe le plus touché
)

# Actions suite à Transformation 1b
df_crimes_par_age_group.show()  # ACTION 5 : Voir la répartition des crimes par âge
df_crimes_par_age_group.count() # ACTION 6 : Nombre de groupes d'âge différents

# =============================
# Transformation 2 : Filtrage des crimes avec arme
# =============================
df_armed = df_spark.filter(
    (col("weapon_desc").isNotNull()) &
    (col("weapon_desc") != "No weapon")
)  # Transformation 2 : conserver uniquement les crimes avec arme

# Actions suite à Transformation 2
df_armed.show(5)  # ACTION 7 : Afficher 5 lignes filtrées
df_armed.count()  # ACTION 8 : Nombre total de crimes avec arme

# =============================
# Transformation 2b : Analyse par type d'arme
# =============================
df_crimes_par_arme = (
    df_armed
        .groupBy("weapon_desc")  # Transformation 2b : regrouper par type d'arme
        .count()                 # compter le nombre de crimes par arme
        .orderBy("count", ascending=False)  # trier du plus fréquent au moins fréquent
)

# Actions suite à Transformation 2b
df_crimes_par_arme.show(10)  # ACTION 9 : Voir les 10 armes les plus utilisées
df_crimes_par_arme.count()   # ACTION 10 : Nombre de types d'armes différents

# =============================
# Transformation 3 : Analyse par zone
# =============================
df_crimes_par_zone = (
    df_spark
        .filter(col("area_name").isNotNull())  # Transformation 3 : filtrer zones valides
        .groupBy("area_name")                  # regrouper par zone
        .count()                               # compter le nombre de crimes par zone
        .orderBy("count", ascending=False)     # trier du plus touché au moins touché
)

# Actions suite à Transformation 3
df_crimes_par_zone.show(10)  # ACTION 11 : Voir les 10 zones les plus touchées
df_crimes_par_zone.count()   # ACTION 12 : Nombre total de zones différentes

# =============================
# Transformation 4 : Heure Moyenne du Crime par Zone et Sexe de la Victime
# =============================
from pyspark.sql.functions import avg, desc

moyenne_heure_par_zone = df_spark.groupBy("area_name", "vict_sex").agg(
    avg(col("heure")).alias("Heure_Moyenne_Crime")  # Transformation 4 : calcul de l'heure moyenne
).orderBy(desc("Heure_Moyenne_Crime"))

# Action suite à Transformation 4
print("\n--- RÉSULTAT 2 : Heure Moyenne du Crime par Zone et Sexe (Top 5) ---")
moyenne_heure_par_zone.show(5, truncate=False)  # ACTION 13 : Afficher top 5

# =============================
# Transformation 5 : Distribution des Crimes par Type de Lieu et Groupe d'Âge
# =============================
from pyspark.sql.functions import count

distribution_lieu_age = df_spark.groupBy("premis_desc", "vict_age_group").agg(
    count("*").alias("Nombre_Crimes")  # Transformation 5 : compter le nombre de crimes
).orderBy(desc("Nombre_Crimes"))

# Action suite à Transformation 5
output_base_path = "./output"
output_path_csv = f"{output_base_path}/distribution_lieu_age.csv"
distribution_lieu_age.write.mode("overwrite").csv(output_path_csv, header=True)  # ACTION 14 : Enregistrer en CSV
print(f"\nRésultat 3 enregistré au format CSV dans {output_path_csv}.")

# =============================
# Transformation 6 : Regroupement des données par type de crime commis
# =============================
crimes_par_type = (
    df_spark.groupBy("crm_cd_desc").count().orderBy(F.col("count").desc()))
crimes_par_type.show() # ACTION 15

# =============================
# Transformation 7 : Filtre sur les crimes où une arme est renseignée et non "UNKNOWN"/"NONE"
# =============================
crimes_avec_arme = df_filtre.filter(
    (F.col("weapon_desc").isNotNull()) &
    (~F.col("weapon_desc").isin("UNKNOWN", "NONE", "NA"))
)
# afficher quelques lignes pour vérifier: 
crimes_avec_arme.select("weapon_desc", "crm_cd_desc", "area_name").show(10, truncate=False)
# =============================
# Fin de l'analyse
# =============================
print("\nAnalyse terminée. L'interprétation des résultats sera incluse dans le Rapport écrit et la Présentation orale.")
