package org.pycodemaker.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object HelloSparkSql extends Serializable{
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder()
      .appName("HelloSparkSQL")
      .master("local[3]")
      .getOrCreate()

    val surveyDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/sample.csv")

    surveyDF.createOrReplaceTempView("survey_table_view")
    val q = "SELECT Country, count(1) AS count FROM survey_table_view WHERE AGE<40 GROUP BY Country"
    val countDF = spark.sql(q)
    logger.info(countDF.collect().mkString(","))


  }



}
