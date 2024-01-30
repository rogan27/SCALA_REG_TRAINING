package org.example.scala
import org.apache.spark.sql.functions.{explode, explode_outer}
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DataFrameCode {
  val dw_dir = "file:///G:\\Ashok\\TRAININGS\\JIGSAW\\TEST_FILES\\dw_dataset"
  val sales_1_path = dw_dir + "\\sales_1.csv"
  val sales_2_path = dw_dir + "\\sales_2.csv"
  val product_path = dw_dir + "\\product_meta.csv"
  def main(args: Array[String]): Unit = {
    val winutilPath = "C:\\softwares\\winutils" //\\bin\\winutils.exe"; //bin\\winutils.exe";

    if (System.getProperty("os.name").toLowerCase.contains("win")) {
      System.out.println("Detected windows")
      System.setProperty("hadoop.home.dir", winutilPath)
      System.setProperty("HADOOP_HOME", winutilPath)
    }

    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()

    spark.read.text()

    import spark.implicits._

    val arrayData = Seq(
      Row("James",List("Java","Scala"),Map("hair"->"black","eye"->"brown")),
      Row("Michael",List("Spark","Java",null),Map("hair"->"brown","eye"->null)),
      Row("Robert",List("CSharp",""),Map("hair"->"red","eye"->"")),
      Row("Washington",null,null),
      Row("Jefferson",List(),Map())
    )

    val arraySchema = new StructType()
      .add("name",StringType)
      .add("knownLanguages", ArrayType(StringType))
      .add("properties", MapType(StringType,StringType))

    //    val df2 = spark.read.schema(arraySchema).csv()
    val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData),arraySchema)
    df.printSchema()
    df.show(false)
    //df.select($"name",explode($"knownLanguages"))
      //.show(false)

    df.select($"name", explode_outer($"knownLanguages"))
      .show(false)

    //simple_cube(spark)
    //joined_write(spark)

  }

  def simple_cube(spark: SparkSession): Unit = {
    val sales1Df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path)
    val sales2Df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_2_path)
    val unionDf = sales1Df.union(sales2Df)
    val prodDf: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(product_path)
    val joinedDf = prodDf.join(unionDf, "item_id")

    joinedDf.show()
    val cubedDf = joinedDf.cube("product_name", "date_of_sale").sum()
    cubedDf.orderBy("product_name").show()
  }

  def joined_write(spark: SparkSession): Unit = {
    val sales1Df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path)
    val sales2Df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_2_path)
    val unionDf = sales1Df.union(sales2Df)
    val prodDf: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(product_path)
    val joinedDf = prodDf.join(unionDf, "item_id")

    joinedDf.write.mode("overwrite")
      .option("header", "true")
      .csv("file:///C:/Training/TVS/dw/output_csv/joined_csv")

    //joinedDf.write.format("csv").option("path", "C:/Training/TVS/dw/output_csv/joined_csv")
  }

  }
