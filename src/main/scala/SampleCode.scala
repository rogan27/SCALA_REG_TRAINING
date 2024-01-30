package org.example.scala

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object SampleCode {
  def main(args: Array[String]) = {
    val wintilpath = "G:\\Ashok\\TRAININGS\\JIGSAW\\PACKAGES\\winutils"
    val dw_dir = "file:///G:\\Ashok\\TRAININGS\\JIGSAW\\TEST_FILES\\dw_dataset"
    val sales_1_path = dw_dir+"\\sales_1.csv"
    val sales_2_path = dw_dir+"\\sales_2.csv"
    val prod_meta_path = dw_dir+"\\product_meta.csv"

    if(System.getProperty("os.name").toLowerCase.contains("win")){
      System.out.println("Detected Windows")
      System.setProperty("hadoop.home.dir",wintilpath)
      System.setProperty("HADOOP_HOME",wintilpath)
    }

    val spark = SparkSession.builder().appName("SCALA_SBT")
      .master("local[*]")
      .getOrCreate()

    val sales1df = spark.read.option("header","true").option("inferSchema","true").csv(sales_1_path)
    val df1 = sales1df.select("item_qty","unit_price","total_amount")
    val df2 = df1.withColumn("actual_amount", df1.col("unit_price") * df1.col("item_qty"))
    val df3 = df2.withColumn("Discount", df2.col("actual_amount") - df2.col("total_amount"))

    val smtotal = df3.agg(Map("total_amount" -> "sum","Discount" -> "sum")).withColumnsRenamed(
      Map("sum(total_amount)" -> "total_sum","sum(Discount)" -> "Discount_sum")
    )



    // Percentage Calc
    /*
    val pctTotal = smtotal.withColumn("Percentage", smtotal.col("Discount_sum") / smtotal.col("total_sum") * 100)
    pctTotal.show()
    */

    // union DataFrames
    /*
    val sales2df = spark.read.option("header","true").option("inferSchema","true").csv(sales_2_path)
    val uniondf = sales1df.union(sales2df)
    //println(uniondf.count())
    */

    // Joining DataFrames
    /*
    val prod_metadf = spark.read.option("header","true").option("inferSchema","true").csv(prod_meta_path)
    val joinedDF = prod_metadf.join(uniondf,"item_id" )
    joinedDF.show()
    */

    // SQL Like Exection Plan dring Qery Data
    //joinedDF.explain(extended = true)

    // Extracting a value from a Dataframe
    /*
    df3.printSchema()
    val r1 : Row = df3.first()
    val firstval = r1.getInt(3)
    println(firstval)
    */

    /* Column Renaming
    val total = sales1df.agg(sum("total_amount")).withColumnRenamed("total_amount","Modified_total_amount")
    */

    /*
    sales1df.printSchema()
    val sales1dfcasted = sales1df.withColumn("Casted_Date",sales1df.col("date_of_sale").cast("String"))
    sales1dfcasted.printSchema()

    val total = sales1df.agg(sum("total_amount"))
    total.show()
    //sales1dfcasted.show()
    */

  }

  def Complex_Joins (spark : SparkSession) ={

  }
}
