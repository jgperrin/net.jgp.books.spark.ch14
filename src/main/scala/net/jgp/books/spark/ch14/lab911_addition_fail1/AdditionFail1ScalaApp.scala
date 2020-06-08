package net.jgp.books.spark.ch14.lab911_addition_fail1

import java.util.ArrayList

import org.apache.spark.sql.functions.{callUDF, col}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}

/**
 * Additions via UDF.
 *
 * @author rambabu.posa
 */
object AdditionFail1ScalaApp {

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

    spark.udf.register("add", new IntegerAdditionScalaUdf, DataTypes.IntegerType)
    spark.udf.register("add", new StringAdditionScalaUdf, DataTypes.StringType)

    var df = createDataframe(spark)
    df.show(false)

    df = df.withColumn("concat", callUDF("add", col("fname"), col("lname")))
    df.show(false)

    // The next operation will fail with an error:
    // Exception in thread "main" org.apache.spark.SparkException: Failed to
    // execute user defined function($anonfun$261: (int, int) => string)

    df = df
      .withColumn("score",
        callUDF("add", col("score1"), col("score2")))

    df.show(false)

    spark.stop
  }

  private def createDataframe(spark: SparkSession): Dataset[Row] = {
    val schema: StructType = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("fname", DataTypes.StringType, false),
      DataTypes.createStructField("lname", DataTypes.StringType, false),
      DataTypes.createStructField("score1", DataTypes.IntegerType, false),
      DataTypes.createStructField("score2", DataTypes.IntegerType, false)))

    val rows = new ArrayList[Row]
    rows.add(RowFactory.create("Jean-Georges", "Perrin", int2Integer(123), int2Integer(456)))
    rows.add(RowFactory.create("Jacek", "Laskowski", int2Integer(147), int2Integer(758)))
    rows.add(RowFactory.create("Holden", "Karau", int2Integer(258), int2Integer(369)))

    spark.createDataFrame(rows, schema)
  }


}
