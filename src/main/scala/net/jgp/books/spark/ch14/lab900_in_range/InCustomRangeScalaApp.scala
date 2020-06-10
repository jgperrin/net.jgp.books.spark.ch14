package net.jgp.books.spark.ch14.lab900_in_range

import java.util.ArrayList

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession,functions=>F}

/**
 * Custom UDF to check if in range.
 *
 * @author rambabu.posa
 */
object InCustomRangeScalaApp {

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

    spark.udf.register("inRange", new InRangeScalaUdf, DataTypes.BooleanType)

    var df: Dataset[Row] = createDataframe(spark)
    df.show(false)

    df = df
      .withColumn("date", F.date_format(F.col("time"), "yyyy-MM-dd HH:mm:ss.SSSS"))
      .withColumn("h", F.hour(F.col("date")))
      .withColumn("m", F.minute(F.col("date")))
      .withColumn("s", F.second(F.col("date")))
      .withColumn("event", F.expr("h*3600 + m*60 +s"))
      .drop("date","h","m","s")

    df.show(false)

    df = df
      .withColumn("between",
        F.callUDF("inRange", F.col("range"), F.col("event")))

    df.show(false)

    spark.stop

  }

  private def createDataframe(spark: SparkSession): Dataset[Row] = {
    val schema: StructType = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("id", DataTypes.StringType, false),
      DataTypes.createStructField("time", DataTypes.StringType, false),
      DataTypes.createStructField("range", DataTypes.StringType, false)))

    val rows = new ArrayList[Row]
    rows.add(RowFactory.create("id1", "2019-03-11 05:00:00", "00h00-07h30;23h30-23h59"))
    rows.add(RowFactory.create("id2", "2019-03-11 09:00:00", "00h00-07h30;23h30-23h59"))
    rows.add(RowFactory.create("id3", "2019-03-11 10:30:00", "00h00-07h30;23h30-23h59"))

    spark.createDataFrame(rows, schema)
  }

}
