package com.example

import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._



object BostonCrime extends App{

//  def countItems(inputList: List[Any]): Map[Any, Int] = {
//    inputList.groupBy(identity).mapValues(_.size)
//  }
//  val l = List('a', 'a', 'b', 'c')
//  println(countItems(l))


  val source1Folder: String = args(0)
  val source2Folder: String = args(1)
  val resultFolder: String = args(2)

  val spark =   SparkSession.builder()
    .config("spark.sql.autoBroadcastJoinThreshold", 0)
    .master("local[*]")
    .getOrCreate()
  def sc = spark.sparkContext
  import spark.implicits._

  val crimeFacts = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(source1Folder)

  val offense = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(source2Folder)
    .withColumn("crime_group", trim(split($"NAME", "-")(0)))
  val offenseCodesBroadcast = broadcast(offense)

  val crime = crimeFacts
    .join(offenseCodesBroadcast, $"CODE" === $"OFFENSE_CODE")


  val district_1 = crime
    .groupBy($"DISTRICT")
    .agg(
      count($"INCIDENT_NUMBER").alias("Crime_sum"),
      avg($"Lat").alias("Lat_avg"),
      avg($"Long").alias("Long_avg")
    )

  val window_1: WindowSpec = Window.partitionBy($"DISTRICT").orderBy($"count".desc)
  val window_2: WindowSpec = Window.partitionBy($"DISTRICT")
  val district_2 = crime
    .groupBy($"DISTRICT", $"YEAR", $"MONTH")
    .agg(count($"INCIDENT_NUMBER").alias("count"))

    .withColumn("rn", row_number().over(window_1))
    .withColumn("month_count", count($"DISTRICT").over(window_2))
    .filter(abs($"rn"-($"month_count"+1)/2) <= 0.5)

    .groupBy($"DISTRICT")
    .agg(avg($"count").alias("median"))

  val district_3 = crime
    .groupBy($"DISTRICT", $"crime_group")
    .agg(count($"INCIDENT_NUMBER").alias("count"))

    .withColumn("rn", row_number().over(window_1))
    .filter($"rn" <= 3)

    .groupBy($"DISTRICT")
    .agg(collect_list($"crime_group").alias("group_list"))
    .withColumn("group_list", concat_ws(", ", $"group_list"))


  val result = district_1
    .join(district_2, "DISTRICT")
    .join(district_3, "DISTRICT")

  result
    .repartition(1)
    .write.format("parquet")
    .mode("OVERWRITE")
    .save(resultFolder)

  spark.stop()

}
