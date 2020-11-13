package com.scienaptic

import mist.api._
import mist.api.dsl._
import mist.api.encoding.defaults._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, LocalFileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
 * Application to convert parquet files within a specific folder pattern to delta format
 */
object Application extends MistFn with Logging {

  override def handle: Handle = {
    withArgs(
      arg[String]("host", "localhost"), arg[Int]("port", 9000), arg[String]("filesPath", "/data")
    )
      .onSparkSession { (host: String, port: Int, filesPath: String, spark: SparkSession) => {
        val defaultFs = s"hdfs://$host:$port"

        val conf: Configuration = new Configuration()

        // File system implementations.
        conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
        conf.set("fs.file.impl", classOf[LocalFileSystem].getName)
        // nameNode host and port
        conf.set("fs.defaultFS", defaultFs)

        //  conf.addResource(new Path("file://" + "/usr/local/hadoop/etc/hadoop" + "/core-site.xml")); // Replace with actual path
        //  conf.addResource(new Path("file://" + "/usr/local/hadoop/etc/hadoop" + "/hdfs-site.xml")); // Replace with actual path

        val fileSystem: FileSystem = FileSystem.get(conf)

        import scala.util.matching.Regex
        val pattern: Regex = raw"""hdfs://$host:$port$filesPath/[0-9]+/[0-9]+/scores/[0-9]+""".r

        @tailrec
        def rec(array: List[FileStatus], list: List[String]): List[String] = {
          array match {
            case Nil => list
            case ::(head, tl) =>
              if (pattern.findFirstIn(head.getPath.toString).isDefined) {
                rec(tl, head.getPath.toString :: list)
              } else if (head.isDirectory && !head.getPath.toString.contains("/models/")) {
                rec(tl ++ fileSystem.listStatus(head.getPath).toList, list)
              } else {
                rec(tl, list)
              }
          }
        }
        logger.info(s"file path: $filesPath")
        val filePaths = rec(fileSystem.listStatus(new Path(filesPath)).toList, List.empty[String])
        val totalFiles = filePaths.length
        logger.info(s"total files available: $totalFiles")

        import io.delta.tables._
        filePaths.foldLeft(0) { (acc, path) =>
          logger.info(s"converting file number ${acc + 1} out of $totalFiles to delta: $path")

          // Convert un-partitioned parquet table at specified path
          Try(DeltaTable.convertToDelta(spark, s"parquet.`$path`")) match {
            case Failure(exception) =>
              logger.error(s"Error converting to delta format: $path")
              logger.error(exception.getMessage)
            case Success(_) => ()
          }
          acc + 1

        }
      }
      }
  }.asHandle

}
