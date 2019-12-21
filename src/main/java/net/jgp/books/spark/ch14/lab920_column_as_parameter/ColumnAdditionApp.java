package net.jgp.books.spark.ch14.lab920_column_as_parameter;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Additions via UDF.
 * 
 * @author jgp
 */
public class ColumnAdditionApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    ColumnAdditionApp app = new ColumnAdditionApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Column addition")
        .master("local[*]")
        .getOrCreate();
    spark.udf().register(
        "add",
        new ColumnAdditionUdf(),
        DataTypes.IntegerType);

    Dataset<Row> df = createDataframe(spark);
    df.show(false);

    df = df
        .withColumn(
            "sum",
            callUDF("add", col("fname"), col("lname")));
    df.show(false);

  }

  private static Dataset<Row> createDataframe(SparkSession spark) {
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "rule",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "c0",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "c1",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "c2",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "c3",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "c4",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "c5",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "c6",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "c7",
            DataTypes.IntegerType,
            false), });

    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create("c1+c3", 1, 2, 4, 8, 16, 32, 64, 128));

    return spark.createDataFrame(rows, schema);
  }
}