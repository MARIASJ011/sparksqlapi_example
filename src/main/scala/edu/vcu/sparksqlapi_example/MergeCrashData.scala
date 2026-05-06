package edu.vcu.sparksqlapi_example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._

object MergeCrashData extends App{
    val spark = SparkSession.builder()
        .appName("OD Causes Example")
        .master("local[*]")
        .getOrCreate()

    // this import allows us to use Scala primitives in Spark datasets. Scala primitives are Java primitives, 
    // which are not serializable. 
    import spark.implicits._
    
    // read OD data
    val odData = spark.read.format("csv")
        .option("header", "true")
        .option("sep", ",")
        .load(args(0))

    // read crash data
    val crash = spark.read.format("csv")
        .option("header", "true")
        .option("sep", ",")
        .load(args(1))

    // add schema: this is essential for working with datasets. a schema is a case class that describes an abstract row. 
    // this is just like the column definitions you provide when writing a SQL CREATE TABLE statement.
    // this map operation 1. projects (i.e. SELECTs) fields, and applies preprocessing transformations to them
    // this is done subject to the respective schema, and critically, respects the types of elements in the case class
    // that describes said schema. so the case class odSchema expects values in a particular order each of a given type
    val selectedOdData = odData.map({
        rw => odSchema(
            rw.getAs[String]("Overdose ED Visit Year").toInt,
            rw.getAs[String]("Overdose ED Visit Drug Type"), 
            rw.getAs[String]("Overdose ED Visit Patient Geography Name").toLowerCase(), 
            rw.getAs[String]("Overdose ED Visit Rate per 10,000 visits").toFloat
        )
    })

    val selectedCrashData = crash.map({
        rw => crashSchema(
            rw.getAs[String]("Crash Year").toInt,
            GeoClean.cleanGeography(rw.getAs[String]("Physical Juris Name")), 
            rw.getAs[String]("Persons Injured").toInt, 
            rw.getAs[String]("Alcohol?"), 
            rw.getAs[String]("Drug Related?")
        )
    })

    val alcoholCrashes = selectedCrashData.filter(selectedCrashData("alcohol") === "Yes")

    // how many people were injured in each year, in each geography in a drug-related car accident?
    val drugCrashInjury = selectedCrashData.filter(x=> x.drugs == "Yes")
        .groupBy("year", "geography")
        .agg(sum("personsInjured"))
        .sort("year","geography")
    
    // for each geography and in each year, compare ED overdose visit rates to the number of persons injured in a drug related car accident
    val crashODs = selectedOdData.join(drugCrashInjury, Seq("geography", "year"))

    // write the reuslts to disk
    crashODs.write.csv(args(2))
}
