package net.jgp.books.spark.ch14.lab210_library_open_sql

import java.util.ArrayList

import net.jgp.books.spark.ch14.lab200_library_open.IsOpenScalaUdf
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession, functions => F}

/**
 * Custom UDF to check if in range.
 *
 * @author rambabu.posa
 */
object OpenedLibrariesSqlScalaApp {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    /**
     * The processing code.
     */
    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("Custom UDF to check if in range")
      .master("local[*]")
      .getOrCreate

    spark.udf.register("isOpen", new IsOpenScalaUdf, DataTypes.BooleanType)

    val librariesDf = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .option("encoding", "cp1252")
      .load("data/south_dublin_libraries/sdlibraries.csv")
      .drop("Administrative_Authority", "Address1", "Address2", "Town", "Postcode",
        "County", "Phone", "Email", "Website", "Image", "WGS84_Latitude", "WGS84_Longitude")

    librariesDf.show(false)
    librariesDf.printSchema()

    val dateTimeDf = createDataframe(spark)
    dateTimeDf.show(false)
    dateTimeDf.printSchema()

    val df = librariesDf.crossJoin(dateTimeDf)
    df.createOrReplaceTempView("libraries")
    df.show(false)

    val sqlQuery = "SELECT Council_ID, Name, date, " +
      "isOpen(Opening_Hours_Monday, Opening_Hours_Tuesday, " +
      "Opening_Hours_Wednesday, Opening_Hours_Thursday, " +
      "Opening_Hours_Friday, Opening_Hours_Saturday, 'closed', date) AS open" +
      " FROM libraries "

    // Using SQL
    val finalDf = spark.sql(sqlQuery)

    finalDf.show()

    spark.stop
  }

  private def createDataframe(spark: SparkSession): Dataset[Row] = {
    val schema: StructType = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("date_str", DataTypes.StringType, false)))

    val rows = new ArrayList[Row]
    rows.add(RowFactory.create("2019-03-11 14:30:00"))
    rows.add(RowFactory.create("2019-04-27 16:00:00"))
    rows.add(RowFactory.create("2020-01-26 05:00:00"))

    spark.createDataFrame(rows, schema)
      .withColumn("date", F.to_timestamp(F.col("date_str")))
      .drop("date_str")
  }

}
