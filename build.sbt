name := "homework1"

version := "0.0.1"

scalaVersion := "2.11.0"

logBuffered in Test := false

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.2" % "provided",
  "org.apache.spark" %% "spark-hive" % "1.6.2",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.apache.spark" %% "spark-sql" % "1.6.2",
  "com.databricks" %% "spark-csv" % "1.4.0",
  "org.apache.kafka" % "kafka-clients" % "0.9.0.0",
  "org.apache.spark" %% "spark-streaming" % "1.6.2"
)

unmanagedJars in Runtime += file("lib/jpcap.zip")