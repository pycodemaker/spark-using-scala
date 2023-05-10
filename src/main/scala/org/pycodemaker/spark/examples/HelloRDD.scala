package org.pycodemaker.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object HelloRDD extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    // create spark context
    val sparkAppConf = new SparkConf().setAppName("HelloRDD").setMaster("local[3]")
    val sparkContext = new SparkContext(sparkAppConf)

    // read your csv file
    val linesRDD = sparkContext.textFile("data/sample-no-header.csv", 2)

    // give it struct and select only 4 cols
    case class SurveyRecord(Age: Int, Gender: String, Country: String, state: String)
    val colsRDD = linesRDD.map(line => {
      val cols = line.split(",").map(_.trim)
      SurveyRecord(cols(1).toInt, cols(2), cols(3), cols(4))
    })

    // apply filter
    val filteredRDD = colsRDD.filter(r => r.Age >= 40)

    // manually implement the groupBy
    val kvRDD = filteredRDD.map(r => (r.Country, 1))
    val countRDD = kvRDD.reduceByKey((v1, v2) => v1 + v2)

    // collect the result
    logger.info(countRDD.collect().mkString(","))
    sparkContext.stop()

   }
}
