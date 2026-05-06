import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkODAnalysis {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("INFO602_OD_Crash_Analysis")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // ── Load VDH OD data ──────────────────────────────────────────────────────
    val odRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/vdh_od.csv")

    println("=== VDH OD columns ===")
    odRaw.printSchema()

    // ── Load Crash data ───────────────────────────────────────────────────────
    val crashRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/crash_data.csv")

    println("=== Crash columns ===")
    crashRaw.printSchema()

    // ── Clean OD data ─────────────────────────────────────────────────────────
    val odClean = odRaw
      .filter($"Overdose ED Visit Patient Geography Level" === "Locality")
      .withColumn("county",
        trim(upper(regexp_replace(
          col("Overdose ED Visit Patient Geography Name"),
          "(?i)\\s+(city|county)$", ""))))
      .withColumn("year",   col("Overdose ED Visit Year").cast("int"))
      .withColumn("drug_type", col("Overdose ED Visit Drug Type"))
      .withColumn("od_count",
        col("Overdose ED Visit Count").cast("double"))
      .withColumn("od_rate",
        col("Overdose ED Visit Rate per 10,000 visits").cast("double"))
      .select("county", "year", "drug_type", "od_count", "od_rate")
      .filter($"od_rate".isNotNull)

    println(s"OD clean row count: ${odClean.count()}")
    odClean.show(5, truncate = false)

    // ── Clean Crash data ──────────────────────────────────────────────────────
    val crashClean = crashRaw
      .withColumn("county",
        trim(upper(regexp_replace(
          col("Physical Juris Name"),
          "(?i)^\\d+\\.\\s*", ""))))
      .withColumn("county",
        regexp_replace($"county", "(?i)\\s+(city|county|town of).*$", ""))
      .withColumn("county", trim(upper($"county")))
      .withColumn("year", col("Crash Year").cast("int"))
      .groupBy("county", "year")
      .agg(count("*").alias("crash_count"))

    println(s"Crash clean row count: ${crashClean.count()}")
    crashClean.show(5, truncate = false)

    // ── Join diagnostic ───────────────────────────────────────────────────────
    val odCounties    = odClean.select("county").distinct()
    val crashCounties = crashClean.select("county").distinct()
    val matched = odCounties.join(crashCounties, Seq("county"), "inner")
    println("=== Join diagnostic: how many counties match? ===")
    matched.agg(count("*").alias("matched_counties")).show()

    println("=== Sample OD county names ===")
    odCounties.orderBy("county").show(20, truncate = false)
    println("=== Sample Crash county names ===")
    crashCounties.orderBy("county").show(20, truncate = false)

    // ── Filter OD to "All Drug" only for cleaner analysis ────────────────────
    val odAllDrug = odClean.filter($"drug_type" === "All Drug")

    // ── Analysis A: Statewide OD trend by year ────────────────────────────────
    println("=== Analysis A: Statewide Average OD Rate by Year ===")
    val analysisA = odAllDrug
      .groupBy("year")
      .agg(
        round(avg("od_rate"), 2).alias("avg_od_rate"),
        round(sum("od_count"), 0).alias("total_od_visits"))
      .orderBy("year")

    analysisA.show(20, truncate = false)

    // ── Analysis B: Join OD + Crash, bucket by crash volume ──────────────────
    println("=== Analysis B: Avg OD Rate by Crash Volume Bucket ===")
    val joined = odAllDrug
      .join(crashClean, Seq("county", "year"), "inner")

    val bucketed = joined.withColumn("crash_bucket",
      when($"crash_count" < 100,  lit("Low (<100 crashes)"))
      .when($"crash_count" < 500, lit("Medium (100-499)"))
      .otherwise(                  lit("High (500+)")))

    val analysisB = bucketed
      .groupBy("crash_bucket")
      .agg(
        round(avg("od_rate"), 2).alias("avg_od_rate"),
        count("*").alias("county_year_pairs"))
      .orderBy("avg_od_rate")

    analysisB.show(truncate = false)

    // ── Analysis C: Top 10 counties by average OD rate ───────────────────────
    println("=== Analysis C: Top 10 Counties by Avg OD Rate ===")
    val analysisC = odAllDrug
      .groupBy("county")
      .agg(round(avg("od_rate"), 2).alias("avg_od_rate"))
      .orderBy(desc("avg_od_rate"))

    analysisC.show(10, truncate = false)

    // ── Analysis D: Drug-Related crashes vs OD rate ───────────────────────────
    println("=== Analysis D: Drug-Related Crash Counties vs OD Rate ===")
    val crashWithDrug = crashRaw
      .withColumn("county",
        trim(upper(regexp_replace(
          regexp_replace(col("Physical Juris Name"), "(?i)^\\d+\\.\\s*", ""),
          "(?i)\\s+(city|county|town of).*$", ""))))
      .withColumn("year", col("Crash Year").cast("int"))
      .withColumn("is_drug", when(col("Drug Related?") === "Yes", 1).otherwise(0))
      .groupBy("county", "year")
      .agg(
        count("*").alias("total_crashes"),
        sum("is_drug").alias("drug_crashes"))
      .withColumn("drug_crash_pct",
        round($"drug_crashes" / $"total_crashes" * 100, 2))

    val analysisD = odAllDrug
      .join(crashWithDrug, Seq("county", "year"), "inner")
      .withColumn("high_drug_crash",
        when($"drug_crash_pct" > 2, lit("High Drug Crash %"))
        .otherwise(lit("Low Drug Crash %")))
      .groupBy("high_drug_crash")
      .agg(
        round(avg("od_rate"), 2).alias("avg_od_rate"),
        count("*").alias("n"))
      .orderBy("avg_od_rate")

    analysisD.show(truncate = false)

    println("=== All analyses complete ===")
    spark.stop()
  }
}
