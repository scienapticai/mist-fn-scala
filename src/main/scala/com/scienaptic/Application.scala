package com.scienaptic

import mist.api._
import mist.api.dsl._
import mist.api.encoding.defaults._
import org.apache.spark.sql.SparkSession

object Application extends MistFn with Logging {

  override def handle: Handle = {
    withArgs(
      arg[String]("inputPath")
    )
      .onSparkSession { (path: String, spark: SparkSession) => {
        val filePath = s"file://$path"
        val df = spark.read
          .format("csv")
          .option("header", value = true)
          .load(filePath)
        val fileName = filePath.split("\\.").dropRight(1).mkString(",")
        val outputPath = s"$fileName.parquet"
        df.write.mode("overwrite").parquet(outputPath)
        outputPath
      }
      }
  }.asHandle

}
