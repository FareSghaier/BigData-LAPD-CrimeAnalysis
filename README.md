# Big Data Crime Analysis — LAPD Dataset

Analysis of **1,004,991 real crime records** from Los Angeles (2020–present) using **Apache Spark (PySpark)** on a Hadoop cluster.

---

## What This Project Does

This project builds a complete big data pipeline that:
1. Cleans and prepares over 1 million raw crime records from the LAPD open dataset
2. Loads and processes the data using Apache Spark on a Hadoop cluster
3. Answers real analytical questions about crime patterns in Los Angeles

**Questions answered:**
- What are the most frequent crime types in LA since 2022?
- Which weapon types are most commonly used?
- Which age groups and city zones are most affected?
- What are the temporal patterns of crime (hour of day, day of week)?

**Key findings:**
- Most frequent crime: Vehicle Stolen (66,350 cases)
- Most affected age group: 25–40 years old (peak at age 30)
- Most common weapon: Strong-arm (hands, fists)
- Most affected zones: Central, 77th Street, Pacific

---

## Dataset

| | |
|---|---|
| **Source** | Los Angeles Open Data Portal |
| **Link** | https://data.lacity.org/Public-Safety/Crime-Data-from-2020-to-Present/2nrs-mtv8 |
| **Size** | 1,004,991 rows — 16 columns |
| **Format** | CSV |

> The dataset is not included in this repo (too large). Download it from the link above.

---

## Technologies Used

- Python 3
- Apache Spark / PySpark (DataFrames + RDD)
- Hadoop cluster
- Pandas & NumPy
- Jupyter Notebook
- Google Colab

---

## Files

| File | Description |
|---|---|
| `cleaned_crimes.ipynb` | **Step 1** — Pandas cleaning notebook: column selection, timestamp engineering, missing value imputation, feature engineering |
| `crime_clean.py` | **Step 2** — PySpark cleaning script optimized for distributed processing |
| `crime_analysis.py` | **Step 3** — PySpark analysis on Hadoop: 7 transformations, 4 actions, aggregations, and results export |

---

## How the Pipeline Works

```
Raw CSV (1M+ rows)
       ↓
cleaned_crimes.ipynb   →  cleans data with Pandas (0 missing values after cleaning)
       ↓
crime_clean.py         →  PySpark distributed cleaning
       ↓
crime_analysis.py      →  Spark analysis on Hadoop cluster → output CSV results
```

---

## Spark Transformations Used

`filter` · `groupBy` · `agg` · `orderBy` · `map` · `reduceByKey` · `join`

**Actions:** `count()` · `show()` · `take()` · `write.csv()`

---

## How to Run

**1. Install dependencies**
```bash
pip install pyspark pandas numpy jupyter
```

**2. Download the dataset** from the link above and place it in the project folder as:
```
Crime_Data_from_2020_to_Present.csv
```

**3. Run the cleaning notebook**
```bash
jupyter notebook cleaned_crimes.ipynb
```

**4. Run the Spark analysis**
```bash
spark-submit crime_analysis.py
```

---

## Author

**Fares Sghaier** — Applied Data Science, La Cité College, Ottawa, Canada
