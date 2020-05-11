package net.jgp.books.spark.ch14.lab910_addition

import java.util.ArrayList

import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession, functions => F}

/**
 * Additions via UDF.
 *
 * @author rambabu.posa
 */
object AdditionScalaStyleApp {

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
      .appName("Addition")
      .master("local[*]")
      .getOrCreate

    spark.udf.register("add_int", (a:Int,b:Int) => a+b)
    spark.udf.register("add_string", (a:String,b:String) => a+b)

    var df = createDataframe(spark)
    df.show(false)

    df = df
      .withColumn("concat",
        F.callUDF("add_string", F.col("fname"), F.col("lname")))

    df.show(false)

    df = df
      .withColumn("score",
      F.callUDF("add_int", F.col("score1"), F.col("score2")))

    df.show(false)

    spark.stop
  }

  private def createDataframe(spark: SparkSession): Dataset[Row] = {
    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("fname", DataTypes.StringType, false),
      DataTypes.createStructField("lname", DataTypes.StringType, false),
      DataTypes.createStructField("score1", DataTypes.IntegerType, false),
      DataTypes.createStructField("score2", DataTypes.IntegerType, false)))

    val rows = new ArrayList[Row]
    rows.add(RowFactory.create("Jean-Georges", "Perrin", 123, 456))
    rows.add(RowFactory.create("Jacek", "Laskowski", 147, 758))
    rows.add(RowFactory.create("Holden", "Karau", 258, 369))

    spark.createDataFrame(rows, schema)
  }

}
