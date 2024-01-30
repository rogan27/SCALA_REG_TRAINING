package org.example.scala

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{col, date_format, hour, month, to_date, unix_timestamp, year}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}

import java.sql.Date


object Assignment1 {
  val dw_dir = "file:///G:\\Ashok\\TRAININGS\\JIGSAW\\ASSIGNMENTS\\1st Assignment\\INPUTS\\dataset"
  val superstore_Returns_path = dw_dir + "\\Global Superstore Sales - Global Superstore Returns.csv"
  val superstore_Sales_path = dw_dir + "\\Global Superstore Sales - Global Superstore Sales.csv"
  case class superstore_Sales_Class(OrderID: String, OrderDate: Date, ShipMode: String , Segment: String, City: String
                                    ,State: String, Country: String , Category: String, SubCategory: String, Sales: String
                                    ,Quantity: Int, Discount: Double,Profit: Double, ShippingCost: Double, Returns: String)
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


    windows_agg(spark)
  }

  def windows_agg(spark: SparkSession): Unit = {
    val superstore_ReturnsDf = spark.read.option("header", "true").option("inferSchema", "true").csv(superstore_Returns_path)

    val tempSuperstore_SalesDf = spark.read.option("header", "true").option("inferSchema", "true").csv(superstore_Sales_path)
    val superstore_SalesDf = tempSuperstore_SalesDf
      .withColumn("Profit_New", functions.regexp_replace(tempSuperstore_SalesDf.col("Profit"), "[/[$]/g]", ""))
      //.withColumn("date",date_format(unix_timestamp(col("Order Date"), "MM/dd/yyyy").cast(TimestampType), "yyyyMMdd"))
      .withColumn("date", date_format(to_date(tempSuperstore_SalesDf.col("Order Date"), "MM/dd/yyyy"), "yyyyMMdd"))
//    val dfWithDate = superstore_SalesDf.withColumn("date", to_date("dateString"))
//    val dfWithYear = dfWithDate.withColumn("year", year("date"))
//    val actualData = superstore_SalesDf.select(to_date(col("Order Date"), "MM-dd-yyyy").as("to_date"),
//      month(to_date(superstore_SalesDf.col("Order Date"), "MM-dd-yyyy")).as("Month"))
    superstore_SalesDf.show()
    //val joinedDf = superstore_ReturnsDf.join(superstore_SalesDf, "Order ID")

    //joinedDf.show()
    //val cubedDf = joinedDf.cube("Profit", "Quantity").sum()
    //cubedDf.orderBy("Country").show()

/*
    val aggSpec: WindowSpec = Window.partitionBy("date_of_sale").
      orderBy(functions.col("total_amount").desc)
    val rankSpecDf: Dataset[Row] = superstore_ReturnsDf
      .withColumn("date_wise_rank", functions.dense_rank.over(rankSpec))
      .where("date_wise_rank = 1")
    rankSpecDf.show()
*/

  }
}
