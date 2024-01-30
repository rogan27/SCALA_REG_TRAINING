package com.bdec.training.sparkscala

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}

import java.sql.Date

case class Sales (item_id: Int,item_qty: Int,unit_price: Int,total_amount: Int,abc:Int, date_of_sale: Date)
object DevaRefCode {
  val dw_dir = "file:///G:\\Ashok\\TRAININGS\\JIGSAW\\TEST_FILES\\dw_dataset"
  val sales_1_path = dw_dir + "\\sales_1.csv"
  val sales_2_path = dw_dir + "\\sales_2.csv"
  val product_path = dw_dir + "\\product_meta.csv"

  def main(args: Array[String]) = {
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

    //complex_join(spark)

    //dataset_version(spark)

    //windows_agg(spark)

    sql_version(spark)

  }

  def sql_version(spark: SparkSession) = {
    val sales1Df = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path)
    sales1Df.createOrReplaceTempView("sales_table")
    val prodDf = spark.read.option("header", "true").option("inferSchema", "true").csv(product_path)
    prodDf.createOrReplaceTempView("prod_table")
    spark.sql("select * from prod_table pt left anti join sales_table st on pt.item_id = st.item_id").show()
  }
  def windows_agg(spark: SparkSession) = {
    val sales1Df = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path)

    val rankSpec : WindowSpec = Window.partitionBy("date_of_sale").
      orderBy(functions.col("total_amount").desc)
    val simpleSpec : WindowSpec = Window.partitionBy().orderBy("item_id")
    val simpleSpecDf : Dataset[Row] = sales1Df.withColumn("row_number",functions.row_number.over(simpleSpec))
    val rankSpecDf : Dataset[Row] = sales1Df
      .withColumn("date_wise_rank",functions.dense_rank.over(rankSpec))
      .where("date_wise_rank = 1")




    simpleSpecDf.show()


  }
  def dataset_version(spark: SparkSession) = {
    import spark.implicits._
    val sales1Df = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path).as[Sales]
    val sales2Df = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path)
    val salesDs: Dataset[Sales] = sales1Df.as[Sales]
    val finalDs = salesDs.filter(x=> x.unit_price > 10)
     // .withColumn("abc", lit("80"))
     // .withColumn("total", salesDs.col("abc") * salesDs.col("unit_price"))

    val newFinalDs = finalDs.filter("total_amount > 100")

    val collectedSales2: Array[Row] = sales2Df.collect()
    val row_2_1: Row = collectedSales2(0)
    val row_2_1_item_id = row_2_1.getInt(0)
    val row_2_1_item_qty= row_2_1.getInt(1)


    val collectedSales1: Array[Sales] = salesDs.collect()
    val row1: Sales = collectedSales1(0)
    val item_id = row1.item_id

    finalDs.show()
  }

  def complex_join(spark: SparkSession) = {
    val sales1Df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path)
    val sales2Df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_2_path)
    val prodDf: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(product_path)

    val unionDf = sales1Df.union(sales2Df)
    val df3 = unionDf.withColumn("actual_total",
      unionDf.col("item_qty") * unionDf.col("unit_price"))
    val transformedSalesDf = df3.withColumn("discount",
      df3.col("actual_total") - df3.col("total_amount")).filter("unit_price > 1")

    val joinedDf = prodDf.join(transformedSalesDf, "item_id")
    val groupedDf = joinedDf.groupBy("product_type").sum("total_amount")

    //joinedDf.show()
    groupedDf.explain(extended = true)

  }

  def simple_df_ops(spark: SparkSession) = {
    val sales_1_path = dw_dir + "\\sales_1.csv"
    val sales1Df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path)

    val sales_2_path = dw_dir + "\\sales_2.csv"
    val sales2Df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_2_path)

    val unionDf = sales1Df.union(sales2Df)

    val product_path = dw_dir + "\\product_meta.csv"
    val prodDf: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(product_path)

    val joinedDf = prodDf.join(unionDf, "item_id")

    //joinedDf.show()
    joinedDf.explain(extended = true)

    //    val df2 = sales1Df.select("item_qty", "unit_price", "total_amount")
    //    val df3 = df2.withColumn("actual_total", df2.col("item_qty") * df2.col("unit_price"))
    //    val df4 = df3.withColumn("discount", df3.col("actual_total") - df3.col("total_amount"))
    //    df4.show()
    //    //    val r1: Row = sales1Df.first()
    ////    sales1Df.take()
    ////    sales1Df.first()
    ////    sales1Df.collect()
    ////    println(r1.getInt(0))
    //    //sales1Df.printSchema()
    //    //val sales1CastedDf = sales1Df.withColumn("casted_date", sales1Df.col("date_of_sale").cast("string"))
    //    //sales1CastedDf.printSchema()
    //
    //    val total = sales1Df.agg(sum("total_amount")).withColumnRenamed("sum(total_amount)", "sum_total")
    //    val sumTotal = df4.agg(Map("total_amount" -> "sum", "discount" -> "sum")).withColumnsRenamed(
    //      Map("sum(total_amount)" -> "total_amount_sum", "sum(discount)" -> "discount_sum")
    //    )
    //    val pctTotal = sumTotal.withColumn("pct_total",
    //      sumTotal.col("discount_sum")/sumTotal.col("total_amount_sum") * 100
    //    )
    //
    //
    //    pctTotal.show()
    //    total.show()
    //    total.first()

    //    sales1Df.show()

  }
}
