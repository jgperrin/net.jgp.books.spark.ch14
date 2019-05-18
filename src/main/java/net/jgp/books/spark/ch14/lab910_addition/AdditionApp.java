package net.jgp.books.spark.ch14.lab910_addition;

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
public class AdditionApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    AdditionApp app = new AdditionApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Addition")
        .master("local[*]")
        .getOrCreate();
    spark.udf().register(
        "add_int",
        new IntegerAdditionUdf(),
        DataTypes.IntegerType);
    spark.udf().register(
        "add_string",
        new StringAdditionUdf(),
        DataTypes.StringType);

    Dataset<Row> df = createDataframe(spark);
    df.show(false);

    df = df
        .withColumn(
            "concat",
            callUDF("add_string", col("fname"), col("lname")));
    df.show(false);
    
    df = df
        .withColumn(
            "score",
            callUDF("add_int", col("score1"), col("score2")));
    df.show(false);
  }

  private static Dataset<Row> createDataframe(SparkSession spark) {
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "fname",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "lname",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "score1",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "score2",
            DataTypes.IntegerType,
            false) });

    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create("Jean-Georges", "Perrin", 123, 456));
    rows.add(RowFactory.create("Jacek", "Laskowski", 147, 758));
    rows.add(RowFactory.create("Holden", "Karau", 258, 369));

    return spark.createDataFrame(rows, schema);
  }
}