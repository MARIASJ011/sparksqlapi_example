# INFO602 Final Project – SparkODAnalysis

## 📌 Project Overview

This project is a Scala + Apache Spark application developed for the INFO602 final submission. It performs data analysis on:

* 🚗 Virginia crash data
* 🚑 Overdose (OD) incident data

The goal is to identify patterns, trends, and insights from both datasets using Spark DataFrame operations.

---

## 📂 Project Structure

```
sparksqlapi_example/
│
├── data/
│   └── vdh_od.csv                # Overdose dataset (Virginia Department of Health)
│
├── src/main/scala/edu/vcu/sparksqlapi_example/
│   ├── SparkODAnalysis.scala     # Main Spark analysis logic
│   ├── GeoClean.scala            # Data cleaning utilities
│   ├── MergeCrashData.scala      # Crash dataset processing
│   └── odschema.scala            # Schema definitions
│
└── project/                      # Build configurations
```

---

## ⚙️ Technologies Used

* Apache Spark (DataFrame API)
* Scala
* SBT (Scala Build Tool)
* CSV data processing

---

## 📊 Key Functionalities

### 1. Overdose Data Analysis (OD)

* Reads Virginia OD dataset
* Cleans missing/null values
* Aggregates overdose trends by region/date

### 2. Crash Data Processing

* Loads crash-related dataset
* Standardizes fields
* Performs grouping and aggregation for insights

### 3. Geo Data Cleaning

* Cleans location-based fields
* Prepares structured data for analysis

---

## 🚀 How to Run the Project

### Step 1: Open terminal in project folder

```
cd sparksqlapi_example
```

### Step 2: Build project

```
sbt clean compile
```

### Step 3: Run Spark job

```
sbt run
```

---

## 📌 Expected Output

The program outputs:

* Aggregated OD trends
* Crash statistics summaries
* Cleaned and structured datasets

---

## ⚠️ Notes

* Large dataset files (e.g., crash dataset >100MB) were removed due to GitHub limits.
* Only sample/processed datasets are included in the repository.

---

## 👨‍💻 Author

Maria Shine Joseph
V01150456
