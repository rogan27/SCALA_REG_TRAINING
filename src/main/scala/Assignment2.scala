package org.example.scala

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{DataTypes, DateType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import java.sql.Date

object Assignment2 {
  def read_device_temp(spark: SparkSession): DataFrame = {
    val deviceSchema: StructType = new StructType()
      .add(new StructField("device_id", DataTypes.StringType, false))
      .add(new StructField("temperature", DataTypes.StringType, false))
      .add(new StructField("timestamp", DataTypes.TimestampType, false))

    val deviceDf = spark.readStream.option("header", "true").schema(deviceSchema).csv("file:///E:\\JIGSAW\\TEST_FILES\\STREAM_FILES")
    deviceDf
  }
  def read_device_max_temp(spark: SparkSession): DataFrame = {
    val deviceMaxTempSchema: StructType = new StructType()
      .add(new StructField("device_id", DataTypes.StringType, false))
      .add(new StructField("max_temp", DataTypes.StringType, false))

    val deviceMaxDf = spark.readStream.schema(deviceMaxTempSchema).csv("file:///E:\\JIGSAW\\TEST_FILES\\STREAM_FILES")
    deviceMaxDf
  }

  def join_device_temp_expr(spark: SparkSession): Unit = {
    val device_tempDf = read_device_temp(spark)
    val device_tempDfW = device_tempDf.withColumnRenamed("device_id", "temp_device_id")
    val device_Max_tempDf = read_device_max_temp(spark)
    val joinedDf = device_tempDfW.join(device_Max_tempDf, expr(
      "temp_device_id = device_id")
    )

    val query = joinedDf.writeStream.outputMode("append").format("console").start
    query.awaitTermination()
  }

  def join_static(spark: SparkSession): Unit = {
    val max_tempDf = spark.read.option("header", "true").csv("file:///E:\\JIGSAW\\TEST_FILES\\STATIC_FILES\\max_temperature_devices.csv")
    val max_tempDfW = max_tempDf.withColumnRenamed("device_id", "max_device_id")
    val device_tempDf = read_device_temp(spark)
    val joinedWordsDf = device_tempDf.join(max_tempDfW, device_tempDf.col("device_id") === max_tempDfW.col("max_device_id"), "left_outer")
    val query = joinedWordsDf.writeStream.outputMode("complete").format("console").start
    query.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    val winutilPath = "E:\\JIGSAW\\winutils"

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

//     join_static(spark)
//    join_device_temp_expr(spark)

    val query = read_device_temp(spark).writeStream.outputMode("complete").format("console").start
    query.awaitTermination()
  }
}
