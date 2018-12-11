package workshop.wordcount

import java.time.Clock

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

object Product {
  val log: Logger = LogManager.getRootLogger
  implicit val clock: Clock = Clock.systemDefaultZone()

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    log.setLevel(Level.INFO)
    val spark = SparkSession.builder.appName("Spark Word Count").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    val inputPath = if (!args.isEmpty) args(0) else conf.getString("apps.WordCount.input")
    val outputPath = if (args.length > 1) args(1) else conf.getString("apps.WordCount.output")

    run(spark, inputPath, outputPath)

    spark.stop()
  }

  def run(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
    log.info("Reading data: " + inputPath)
    log.info("Writing data: " + outputPath)
    log.info("Application Done: " + spark.sparkContext.appName)
    import org.apache.spark.sql.functions.{count,lit}
    val productsDF = spark.read // Call the read method returning a DataFrame
      .option("inferSchema", "true") // Option telling Spark to infer the schema
      .option("header", "true") // Option telling Spark that the file has a header
      .csv("/data/products.csv")

    val ordersDF = spark.read // Call the read method returning a DataFrame
      .option("inferSchema", "true") // Option telling Spark to infer the schema
      .option("header", "true") // Option telling Spark that the file has a header
      .csv("/data/orders.csv")
      .toDF("MONTH", "ORDER_ID", "SKU_ID", "AMOUNT")

    val resultByMonthNAMEDF = productsDF
      .join(ordersDF, "SKU_ID")
      .select("MONTH", "NAME")

    val resultByMonthDF = resultByMonthNAMEDF
      .select("NAME")
      .groupBy("NAME")
      .agg(count("*") as "count")
      .withColumn("MONTH", lit("ALL"))

    val resultDF = resultByMonthNAMEDF
      .groupBy("MONTH", "NAME")
      .agg(count("*") as "count")
      .orderBy("MONTH", "NAME")
      .select("NAME", "count", "MONTH")

    resultByMonthDF.union(resultDF)
      .orderBy("NAME", "MONTH")
      .write
      .mode("overwrite")
      .option("header", "true")
      .save("/data/result.csv")
  }
}
