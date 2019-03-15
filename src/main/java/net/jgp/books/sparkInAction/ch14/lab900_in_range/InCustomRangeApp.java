package net.jgp.books.sparkInAction.ch14.lab900_in_range;

import static org.apache.spark.sql.functions.*;

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
 * Custom UDF to check if in range.
 * 
 * @author jgp
 */
public class InCustomRangeApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    InCustomRangeApp app = new InCustomRangeApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Custom UDF to check if in range")
        .master("local[*]")
        .getOrCreate();
    spark.udf().register(
        "inRange",
        new InRangeUdf(),
        DataTypes.BooleanType);

    Dataset<Row> df = createDataframe(spark);
    df.show(false);

    df = df
        .withColumn(
            "date",
            date_format(col("time"), "yyyy-MM-dd HH:mm:ss.SSSS"))
        .withColumn("h", hour(col("date")))
        .withColumn("m", minute(col("date")))
        .withColumn("s", second(col("date")))
        .withColumn("event", expr("h*3600 + m*60 +s"))
        .drop("date")
        .drop("h")
        .drop("m")
        .drop("s");
    df.show(false);

    df = df.withColumn("between",
        callUDF("inRange", col("range"), col("event")));
    df.show(false);
  }

  private static Dataset<Row> createDataframe(SparkSession spark) {
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "id",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "time",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "range",
            DataTypes.StringType,
            false) });

    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create("id1", "2019-03-11 05:00:00",
        "00h00-07h30;23h30-23h59"));
    rows.add(RowFactory.create("id2", "2019-03-11 09:00:00",
        "00h00-07h30;23h30-23h59"));
    rows.add(RowFactory.create("id3", "2019-03-11 10:30:00",
        "00h00-07h30;23h30-23h59"));

    return spark.createDataFrame(rows, schema);
  }
}
