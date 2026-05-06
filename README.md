# INFO 602 Final Project — Drug Overdose & Crash Analysis in Virginia

**Maria Shine Joseph**

---

## Proposition

Virginia localities with higher motor vehicle crash counts also exhibit higher
drug overdose ED visit rates — suggesting that substance use is a shared
behavioral driver of both outcomes across the same geographic corridors.

---

## Datasets

| Dataset | Source | Size |
|---------|--------|------|
| VDH PUD Overdose ED Visits by Year and Geography | Virginia Open Data Portal | ~287 KB |
| CrashData Basic | Virginia DMV / VDOT via Virginia Roads | ~761 MB |

> Large files are excluded from this repo per GitHub's size limits.
> Place both CSVs in a `data/` folder before running.

---

## Key Results (Real Spark Output)

**Analysis A — OD Rate Trend by Year**

| Year | Avg OD Rate (per 10k visits) |
|------|------------------------------|
| 2021 | 61.00 |
| 2022 | 59.78 |
| 2023 | 54.26 |
| 2024 | 43.59 |
| 2025 | 39.88 |
| 2026* | 39.04 |

*Partial year data

**Analysis B — OD Rate by Crash Volume Bucket**

| Crash Bucket | Avg OD Rate | N (county-year pairs) |
|--------------|-------------|----------------------|
| Medium (100–499 crashes/yr) | 50.96 | 232 |
| High (500+ crashes/yr) | 49.30 | 95 |
| Low (< 100 crashes/yr) | 41.79 | 53 |

**Analysis C — Top 10 Highest-OD Localities**

| Rank | Locality | Avg OD Rate |
|------|----------|-------------|
| 1 | Amherst | 96.9 |
| 2 | Lynchburg | 96.7 |
| 3 | Portsmouth | 90.57 |
| 4 | Roanoke County, Roanoke City & Salem | 81.35 |
| 5 | Patrick | 78.72 |
| 6 | Buchanan | 76.62 |
| 7 | Grayson County & Galax | 76.43 |
| 8 | Appomattox | 76.17 |
| 9 | Bland | 70.52 |
| 10 | Henry County & Martinsville | 67.60 |

---

## How to Run

```bash
# 1. Clone the repo
git clone https://github.com/[your-username]/info602-od-analysis.git
cd info602-od-analysis

# 2. Add your data files
mkdir data
# Copy vdh_od.csv and crash_data.csv into data/

# 3. Fix Windows Hadoop path (Windows only)
set HADOOP_HOME=C:\hadoop
set hadoop.home.dir=C:\hadoop

# 4. Run
sbt run
# Select SparkODAnalysis when prompted
```

---

## Project Structure
sparksqlapi_example/
├── data/                          ← place CSVs here (not tracked by git)
│   ├── vdh_od.csv
│   └── crash_data.csv
├── src/main/scala/edu/vcu/sparksqlapi_example/
│   ├── SparkODAnalysis.scala      ← main analysis (this project)
│   ├── MergeCrashData.scala       ← professor's example
│   ├── GeoClean.scala             ← professor's example
│   └── odschema.scala             ← professor's example
└── build.sbt

---

## Tech Stack

- Apache Spark 3.1.1
- Scala 2.12.13
- sbt 1.8.2
- Spark SQL (model-free analysis — no regression models)
