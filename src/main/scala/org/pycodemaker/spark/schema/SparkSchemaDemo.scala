package org.pycodemaker.spark.schema
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object SparkSchemaDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Schema Demo")
      .master("local[3]")
      .getOrCreate()

    val flightSchemaStruct = StructType(List(
      StructField("FL_DATE", DateType),
      StructField("OP_CARRIER", StringType),
      StructField("OP_CARRIER_FL_NUM", IntegerType),
      StructField("ORIGIN", StringType),
      StructField("ORIGIN_CITY_NAME", StringType),
      StructField("DEST", StringType),
      StructField("DEST_CITY_NAME", StringType),
      StructField("CRS_DEP_TIME", IntegerType),
      StructField("DEP_TIME", IntegerType),
      StructField("WHEELS_ON", IntegerType),
      StructField("TAXI_IN", IntegerType),
      StructField("CRS_ARR_TIME", IntegerType),
      StructField("ARR_TIME", IntegerType),
      StructField("CANCELLED", IntegerType),
      StructField("DISTANCE", IntegerType)
    ))

    // You can define the schema as DDL as well but not used too much, only for SQL lovers
    val flightSchemaDDL = "FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, " +
      "ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, " +
      "WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"

    val flightTimeCsvDF = spark.read
      .format("csv")
      .option("header", "true")
      //.option("inferSchema", "true")
      .option("mode", "FAILFAST")
      .option("path", "data/flight-time.csv")
      .option("dateFormat", "M/d/y")
      .schema(flightSchemaStruct)
      .load()

    flightTimeCsvDF.show(10)
    flightTimeCsvDF.printSchema()

    val flighTimeJsonDf = spark.read
      .format("json")
      .option("path", "data/flight-time.json")
      .option("dateformat", "M/d/y")
      .schema(flightSchemaDDL)
      .load()

    flighTimeJsonDf.show(20)
    flighTimeJsonDf.printSchema()
  }

}
