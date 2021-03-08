package com.scienaptic

import mist.api._
import mist.api.dsl._
import mist.api.encoding.defaults._

import org.apache.spark.sql.SparkSession

object Application extends MistFn with Logging {

  override def handle: Handle = {
    withArgs(
      arg[String]("inputPath")
    ).onSparkSession { (filePath: String, spark: SparkSession) =>
      {
        val df = spark.read
          .option("header", value = true)
          .csv(filePath)
        val (fileName,_) = filePath splitAt(filePath.lastIndexOf('.'))
        val outputPath = s"$fileName.parquet"
        df.write.mode("overwrite").parquet(outputPath)
        outputPath
      }
    }
  }.asHandle

}
