package org.example.scala

import org.apache.spark.sql.{SparkSession, functions}

object StreamingDemo {

  def join_static(spark: SparkSession): Unit = {
    val wordType = spark.read.option("header", "true").csv("file:///G:\\Ashok\\TRAININGS\\JIGSAW\\TEST_FILES\\test_sample.csv")

    val streamingFiles = spark.readStream.text("file:///C:\\tmp\\text_files")
    val words = streamingFiles.select(functions.explode(functions.split(streamingFiles.col("value"), " ")).alias("word"))

    val joinedWordsDf = words.join(wordType, words.col("word") === wordType.col("word"),joinType = "right_outer")

    val wordCount = joinedWordsDf.groupBy("word").count().alias("word_total")
    val query = wordCount.writeStream.outputMode("complete").format("console").start
    //val query = words.writeStream.outputMode("append").format("console").start
    query.awaitTermination()

  }
  def main(args: Array[String]): Unit = {
    val winutilPath = "G:\\Ashok\\TRAININGS\\JIGSAW\\PACKAGES\\winutils"

    if (System.getProperty("os.name").toLowerCase.contains("win")) {
      System.out.println("Detected windows")
      System.setProperty("hadoop.home.dir", winutilPath)
      System.setProperty("HADOOP_HOME", winutilPath)
    }


    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    spark.sparkContext.setLogLevel("WARN")


    val streamingFiles = spark.readStream.text("file:///G:\\Ashok\\TRAININGS\\JIGSAW\\TEST_FILES\\stream")
    val words = streamingFiles.select(functions.explode(functions.split(streamingFiles.col("value"), " ")).alias("word"))
    val wordCount = words.groupBy("word").count().alias("word_total")
    val query = wordCount.writeStream.outputMode("complete").format("console").start
    //val query = words.writeStream.outputMode("append").format("console").start
    query.awaitTermination(100000)
  }
}
