package net.jgp.books.spark.ch14.lab920_column_as_parameter

import java.util.ArrayList

import org.apache.spark.sql.functions.{array, callUDF}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql._

/**
 * Additions via UDF.
 *
 * @author rambabu.posa
 */
object ColumnAdditionScalaApp {
  private val COL_COUNT = 8

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
      .appName("Column addition")
      .master("local[*]")
      .getOrCreate

    spark.udf.register("add", new ColumnAdditionScalaUdf, DataTypes.IntegerType)

    var df = createDataframe(spark)

    val cols = List[Column]()
    for (i <- 0 until COL_COUNT) {
      cols :+ = df.col("c" + i)
    }

    val col = array(cols:_*)

    df = df.withColumn("sum", callUDF("add", col))
    df.show(false)

    spark.stop
  }

  private def createDataframe(spark: SparkSession): Dataset[Row] = {
    val schema: StructType = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("c0", DataTypes.IntegerType, false),
      DataTypes.createStructField("c1", DataTypes.IntegerType, false),
      DataTypes.createStructField("c2", DataTypes.IntegerType, false),
      DataTypes.createStructField("c3", DataTypes.IntegerType, false),
      DataTypes.createStructField("c4", DataTypes.IntegerType, false),
      DataTypes.createStructField("c5", DataTypes.IntegerType, false),
      DataTypes.createStructField("c6", DataTypes.IntegerType, false),
      DataTypes.createStructField("c7", DataTypes.IntegerType, false)))

    val rows = new ArrayList[Row]
    rows.add(RowFactory.create(1, 2, 4, 8, 16, 32, 64, 128))
    rows.add(RowFactory.create(0, 0, 0, 0, 0, 0, 0, 0))
    rows.add(RowFactory.create(1, 1, 1, 1, 1, 1, 1, 1))

    spark.createDataFrame(rows, schema)
  }

}
