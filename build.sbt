name := "delta-conversion"

version := "0.0.1"

scalaVersion := "2.12.11"

val sparkVersion = "2.4.2"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "io.hydrosphere" %% "mist-lib" % "1.1.1",

  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "io.delta" %% "delta-core" % "0.6.0"
)
