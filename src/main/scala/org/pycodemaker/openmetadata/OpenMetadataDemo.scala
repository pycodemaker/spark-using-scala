package org.pycodemaker.openmetadata
import io.delta.tables._
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}


object OpenMetadataDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Starting OpenMetadataDemo...")

    val spark = SparkSession.builder()
      .appName("OpenMetadataDemo")
      .master("local[3]")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.network.timeout", "10000s")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.fast.upload", "true")
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minio")
      .config("spark.hadoop.fs.s3a.secret.key", "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.history.fs.logDirectory", "s3a://spark/")
      .config("spark.sql.files.ignoreMissingFiles", "true")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
      .config("spark.sql.warehouse.dir","s3a://spark/warehouse/")
      .config("hive.metastore.warehouse.dir","s3a://spark/warehouse/")
      .enableHiveSupport()
      .getOrCreate()

//    val flightDF = spark.read
//      .option("path","data/flight-time.csv")
//      .option("inferSchema","true")
//      .option("header","true")
//      .format("csv")
//      .load()
//
//    flightDF.write
//      .mode(SaveMode.Overwrite)
//      .format("delta")
//      .partitionBy("ORIGIN")
//      .save("output/flights")

//    val flightDF = spark.read
//      .format("delta")
//      .load("s3a://spark/warehouse/flights")
//
//    flightDF.printSchema()

//    spark.sql("CREATE DATABASE IF NOT exists demo")
//    spark.sql("SHOW DATABASES").show()
//
//    val sql_delta_table = """ CREATE EXTERNAL TABLE IF NOT EXISTS demo.flights (FL_DATE STRING,
//                      OP_CARRIER STRING,
//                      OP_CARRIER_FL_NUM INT,
//                      ORIGIN STRING,
//                      ORIGIN_CITY_NAME STRING,
//                      DEST STRING,
//                      DEST_CITY_NAME STRING,
//                      CRS_DEP_TIME INT,
//                      DEP_TIME INT,
//                      WHEELS_ON INT,
//                      TAXI_IN INT,
//                      CRS_ARR_TIME INT,
//                      ARR_TIME INT,
//                      CANCELLED INT,
//                      DISTANCE INT)
//                      PARTITIONED BY (ORIGIN)
//                      ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
//                      STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
//                      OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
//                      LOCATION 's3a://spark/warehouse/flights/'"""
//    spark.sql(sql_delta_table)
//    spark.sql("MSCK REPAIR TABLE demo.flights")
//    spark.sql("ALTER TABLE demo.flights SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)")

      spark.sql("USE demo").show()
      spark.sql("SHOW TABLES").show()
      spark.sql("SELECT * FROM flights LIMIT 50").show()

    logger.info("Finished OpenMetadataDemo!")
    spark.stop()
  }
}
