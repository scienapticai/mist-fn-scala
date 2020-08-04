name := "sample-app"

version := "0.0.1"

scalaVersion := "2.12.11"

val sparkVersion = "2.4.2"

libraryDependencies ++= Seq(
  "io.hydrosphere" %% "mist-lib" % "1.1.1",

  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
)