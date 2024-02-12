package org.example.scala

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

// ASHOK KUMAR DHANAPAL
object Assignment1 {

  def main(args: Array[String]): Unit = {
    val winutilPath = "C:\\softwares\\hadoop3_winutils"

    if (System.getProperty("os.name").toLowerCase.contains("win")) {
      System.out.println("Detected windows")
      System.setProperty("hadoop.home.dir", winutilPath)
      System.setProperty("HADOOP_HOME", winutilPath)
    }

    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()

    sales_agg(spark)
  }

  def sales_agg(spark: SparkSession): Unit = {
    val superstore_ReturnsDf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("Global Superstore Sales - Global Superstore Returns.csv")
    val tempSuperstore_SalesDf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("Global Superstore Sales - Global Superstore Sales.csv")

    val superstore_SalesDf = tempSuperstore_SalesDf
      .withColumn("Profit_New", functions.regexp_replace(tempSuperstore_SalesDf.col("Profit"),"[/[$]/g]","").cast(DoubleType))
      .withColumn("OrderDate",to_date(to_timestamp(col("Order Date"),"M/d/yyyy")))

    val final_SalesDf = superstore_SalesDf
      .withColumn("Month", month(superstore_SalesDf.col("OrderDate")))
      .withColumn("Year", year(superstore_SalesDf.col("OrderDate")))

    val joinedDf = superstore_ReturnsDf.join(final_SalesDf, "Order ID")
    val salesSelectDF = joinedDf.select(col("Month"),col("Year"),col("Category"),col("Sub-Category"),col("Profit_New").alias("Profit"),col("Quantity"))

    val aggSpec: WindowSpec = Window.partitionBy("Category","Sub-Category","Month","Year").
      orderBy(functions.col("Year").desc)

    val finalDf: Dataset[Row] = salesSelectDF
      .withColumn("Total Quantity Sold", functions.sum(salesSelectDF.col("Quantity")).over(aggSpec))
      .withColumn("Total Profit", functions.sum(salesSelectDF.col("Profit")).over(aggSpec))

    finalDf.write.mode("overwrite")
      .option("header", "true")
      .csv("file:///C:/Training/TVS/dw/output_csv/joined_csv")

  }
}
