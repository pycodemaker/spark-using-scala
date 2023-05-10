package org.pycodemaker.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, Row, SparkSession}

case class SurveyRecord(Age: Int, Gender: String, Country: String, state: String)

object HelloDataset extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    // create spark session
    val spark = SparkSession.builder()
      .appName("HelloDataset")
      .master("local[3]")
      .getOrCreate()

    // read you csv file
    val rawDF: Dataset[Row] = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/sample.csv")

    // typesafe dataset
    import spark.implicits._
    val surveyDS:Dataset[SurveyRecord] = rawDF.select("Age", "Gender", "Country", "state").as[SurveyRecord]

    //Type safe Filter
    val filteredDS = surveyDS.filter(r => r.Age < 40)
    //Runtime Filter
    val filteredDF = surveyDS.filter("Age  < 40")

    //Type safe GroupBy
    val countDS = filteredDS.groupByKey(r => r.Country).count()
    //Runtime GroupBy
    val countDF = filteredDF.groupBy("Country").count()

    logger.info("DataFrame: " + countDF.collect().mkString(","))
    logger.info("DataSet: " + countDS.collect().mkString(","))


  }

}
