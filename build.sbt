ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.14"

val sparkVersion = "3.3.1"

val sparkDependencies = Seq(
  "io.delta"                      %% "delta-core"           % "2.3.0",
  "org.apache.spark"              %% "spark-core"           % sparkVersion,
  "org.apache.spark"              %% "spark-sql"            % sparkVersion,
  "org.apache.spark"              %% "spark-hive"           % sparkVersion,
  "org.apache.hadoop"             % "hadoop-aws"            % sparkVersion,

)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

libraryDependencies ++= sparkDependencies ++ testDependencies