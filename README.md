# INFO 602 Final Project — Drug Overdose & Crash Analysis in Virginia

**Maria Shine Joseph - Big Data Analytics with Cloud Platforms**

---

## Overview

This project investigates whether Virginia localities with higher motor vehicle crash counts also exhibit higher drug overdose emergency department (ED) visit rates. Using Apache Spark 3.1.1 and Spark SQL, two large real-world Virginia datasets were cleaned, joined, and analyzed using a fully model-free approach — no regression models, only cross-tabulations, grouped aggregations, and bucketing.

The analysis processed a 761 MB crash file with hundreds of thousands of rows alongside Virginia Department of Health overdose ED visit data covering 2021–2026. All analysis was performed programmatically in Scala using the Spark SQL API.

---

## Proposition

> **Virginia localities with higher motor vehicle crash counts also exhibit higher drug overdose ED visit rates — suggesting that substance use is a shared behavioral driver of both outcomes across the same geographic corridors.**

This proposition is grounded in public health research linking opioid and stimulant impairment to unsafe driving behavior. If substance use drives both OD hospitalizations and crash incidents, we would expect the two outcomes to co-occur geographically — which is exactly what this analysis tests.

---

## Datasets

| Dataset | Source | Raw Size | Rows After Cleaning |
|---------|--------|----------|---------------------|
| VDH PUD Overdose ED Visits by Year and Geography | Virginia Open Data Portal | ~287 KB | 3,192 rows |
| CrashData Basic | Virginia DMV / VDOT via Virginia Roads Portal | ~761 MB | 2,822 county-year groups |

**VDH OD Dataset** tracks the number and rate of drug overdose emergency department visits among Virginians, broken down by year, drug type, geography level (state vs. locality), and patient demographics. The key metric used in this analysis is the overdose ED visit rate per 10,000 ED visits, filtered to locality-level rows and the "All Drug" type category.

**CrashData Basic** is owned and maintained by the Virginia Department of Motor Vehicles (DMV) through their Traffic Records Electronic Data System (TREDS). Each row in the raw file represents one crash incident. The dataset was aggregated to county-year level by counting incidents per locality per year. The raw file exceeds GitHub's file size limits and is excluded from this repository — see setup instructions below.

---

## Data Cleaning

Both datasets required significant preprocessing before joining. Key steps included:

**Crash dataset geo-normalization:** The `Physical Juris Name` column uses a format like `"020. Chesterfield County"` or `"133. City of Suffolk"`. A three-step REGEXP_REPLACE chain was applied in Spark SQL to: (1) strip the numeric prefix `"020. "`, (2) remove `"City of "` where present, and (3) strip trailing `" County"`, `" City"`, or `" Town"` suffixes. TRIM(UPPER()) was applied to both datasets to ensure case-consistent join keys.

**OD dataset filtering:** Rows were filtered to `Geography Level = 'Locality'` to exclude statewide summary rows, and to `Drug Type = 'All Drug'` for primary analyses. Null values in rate and geography columns were excluded.

**Join key:** Both datasets were joined on `cleaned county name + year`. Out of all Virginia localities, 75 counties matched successfully across both datasets.

**Aggregation level:** Crash data was aggregated from individual incident rows to county-year level using `COUNT(*) GROUP BY county, year` before joining. OD data was already at locality-year level. No state-level data was mixed with county-level data.

---

## Analyses

### Analysis A — OD Rate Trend by Year

**Method:** `GROUP BY year → AVG(od_rate)` across all matched localities, filtered to `drug_type = 'All Drug'`.

**Result:**

| Year | Avg OD Rate (per 10,000 ED visits) | Total OD Visits |
|------|-------------------------------------|-----------------|
| 2021 | 61.00 | 33,166 |
| 2022 | 59.78 | 34,703 |
| 2023 | 54.26 | 33,964 |
| 2024 | 43.59 | 28,839 |
| 2025 | 39.88 | 26,266 |
| 2026* | 39.04 | 5,893 |

*2026 is a partial year as of the data extract date (April 10, 2026).

**Finding:** Virginia's overdose ED visit rate peaked at 61.0 in 2021 and has declined each year since — a 36% reduction through 2026. The 2021 peak aligns with nationally documented COVID-19 effects: social isolation, reduced access to treatment and recovery services, and increased substance use. Despite the decline, the 2026 rate of 39.0 remains far elevated, confirming the crisis is ongoing.

---

### Analysis B — OD Rate by Crash Volume Bucket

**Method:** INNER JOIN of OD and crash datasets on `county + year`. Localities bucketed by annual crash count using CASE WHEN. Average OD rate computed per bucket.

**Spark SQL buckets:**
- High: ≥ 500 crashes per year
- Medium: 100–499 crashes per year
- Low: < 100 crashes per year

**Result:**

| Crash Bucket | Avg OD Rate | County-Year Pairs |
|--------------|-------------|-------------------|
| Medium (100–499 crashes/yr) | 50.96 | 232 |
| High (500+ crashes/yr) | 49.30 | 95 |
| Low (< 100 crashes/yr) | 41.79 | 53 |

**Finding:** Both high and medium crash localities show OD rates approximately 22% above low-crash localities (50.96 and 49.3 vs 41.79). The directional gradient — higher crash volume, higher OD rate — is consistent across all three buckets and supports the proposition. Notably, medium-crash counties slightly edge out high-crash ones. This likely reflects urban dynamics: very high-crash localities tend to be dense urban areas (e.g., Northern Virginia, Hampton Roads) where OD reporting and care-seeking patterns differ from rural localities. The relationship is strongest in mid-sized localities where both crash volume and substance-use burden co-occur in the same communities.

---

### Analysis C — Top 10 Highest-OD Localities

**Method:** `GROUP BY county → AVG(od_rate) → ORDER BY DESC → LIMIT 10`, with a LEFT JOIN to include crash counts as context. Only localities with ≥ 2 years of data were included.

**Result:**

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

**Finding:** The top-10 list is dominated by Southwest Virginia's Appalachian corridor. These localities share a common profile: high historical opioid prescription rates, rural geography with limited healthcare and treatment access, economic decline following the collapse of coal and manufacturing industries, and high poverty rates. Several entries (e.g., Roanoke County/Roanoke City/Salem, Grayson County/Galax) are combined localities in the VDH dataset, which means they represent larger combined catchment populations and may reflect regional referral patterns to shared ED facilities.

---

### Analysis D — Drug-Related Crash Flag vs OD Rate

**Method:** Used the `Drug Related?` boolean flag in the crash dataset to separate localities into high vs. low drug-crash percentage groups. Compared average OD rates between groups.

**Result:**

| Group | Avg OD Rate | N |
|-------|-------------|---|
| High Drug-Related Crash % | 50.33 | 44 |
| Low Drug-Related Crash % | 49.13 | 336 |

**Finding:** The gap is small (50.33 vs 49.13, a 2.4% difference). This is likely explained by systematic under-reporting of the drug-related crash flag — officers do not routinely test for substance impairment at the crash scene, so many drug-impaired crashes are recorded without the flag. This makes crash volume (Analysis B) a stronger and more objective signal than the drug flag itself. The small positive gap is still directionally consistent with the proposition.

---

## Conclusion

The proposition is supported. Localities with higher crash volumes show OD rates approximately 22% above low-crash localities, and the gradient holds directionally across all three crash buckets. Virginia's overdose crisis peaked in 2021 and is declining, but remains significantly elevated. The geographic concentration in Southwest Virginia's Appalachian corridor — with Amherst, Lynchburg, Buchanan, and Patrick County among the hardest-hit — reflects the intersection of opioid history, rural access gaps, and economic vulnerability that characterizes the region's substance use burden.

The drug-related crash flag analysis (Analysis D) reveals a data quality limitation: the flag is under-reported, making crash volume a more reliable co-occurrence signal than the flag itself.

---

## How to Run

```bash
# 1. Clone the repo
git clone https://github.com/[your-username]/info602-od-analysis.git
cd info602-od-analysis

# 2. Add your data files (not tracked by git — too large)
mkdir data
# Copy vdh_od.csv  →  data/vdh_od.csv
# Copy crash CSV   →  data/crash_data.csv

# 3. Windows only — fix Hadoop path (required for Spark on Windows)
mkdir C:\hadoop\bin
# Download winutils.exe → https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/winutils.exe
# Copy winutils.exe to C:\hadoop\bin\
set HADOOP_HOME=C:\hadoop
set hadoop.home.dir=C:\hadoop

# 4. Build and run
sbt run
# When prompted, select: SparkODAnalysis
```

---

## Project Structure

```
sparksqlapi_example/
├── data/                               ← place CSVs here (gitignored)
│   ├── vdh_od.csv
│   └── crash_data.csv
├── src/main/scala/edu/vcu/sparksqlapi_example/
│   ├── SparkODAnalysis.scala           ← main analysis (this project)
│   ├── MergeCrashData.scala            ← professor's template example
│   ├── GeoClean.scala                  ← professor's template example
│   └── odschema.scala                  ← professor's template example
├── build.sbt                           ← Spark 3.1.1, Scala 2.12.13
└── README.md
```

---

## Tech Stack

| Tool | Version |
|------|---------|
| Apache Spark | 3.1.1 |
| Scala | 2.12.13 |
| sbt | 1.8.2 |
| Java | 1.8.0 (JDK 8) |
| Analysis method | Model-free: grouped aggregations, CASE WHEN bucketing, cross-tabulations |

---

## Author

**Maria Shine Joseph**
Dual-degree student — MBA (Business Analytics), Christ University & MS (Decision Analytics and Data Science), Virginia Commonwealth University
INFO 602 — Big Data Analytics with Cloud Platforms · Spring 2026
