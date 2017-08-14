name := "SphereSparkApp"

version := "0.1"

scalaVersion := "2.11.11"

lazy val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)