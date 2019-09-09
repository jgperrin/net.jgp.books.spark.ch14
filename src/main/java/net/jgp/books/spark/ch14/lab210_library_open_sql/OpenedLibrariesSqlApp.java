package net.jgp.books.spark.ch14.lab210_library_open_sql;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_timestamp;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import net.jgp.books.spark.ch14.lab200_library_open.IsOpenUdf;

/**
 * Custom UDF to check if in range.
 * 
 * @author jgp
 */
public class OpenedLibrariesSqlApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    OpenedLibrariesSqlApp app = new OpenedLibrariesSqlApp();
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
    spark
        .udf()
        .register(
            "isOpen",
            new IsOpenUdf(),
            DataTypes.BooleanType);

    Dataset<Row> librariesDf = spark.read().format("csv")
        .option("header", true)
        .option("inferSchema", true)
        .option("encoding", "cp1252")
        .load("data/south_dublin_libraries/sdlibraries.csv")
        .drop("Administrative_Authority")
        .drop("Address1")
        .drop("Address2")
        .drop("Town")
        .drop("Postcode")
        .drop("County")
        .drop("Phone")
        .drop("Email")
        .drop("Website")
        .drop("Image")
        .drop("WGS84_Latitude")
        .drop("WGS84_Longitude");
    librariesDf.show(false);
    librariesDf.printSchema();

    Dataset<Row> dateTimeDf = createDataframe(spark);
    dateTimeDf.show(false);
    dateTimeDf.printSchema();

    Dataset<Row> df = librariesDf.crossJoin(dateTimeDf);
    df.createOrReplaceTempView("libraries");
    df.show(false);

    // Using SQL
    Dataset<Row> finalDf = spark.sql(
        "SELECT Council_ID, Name, date, "
        + "isOpen("
        + "Opening_Hours_Monday, Opening_Hours_Tuesday, "
        + "Opening_Hours_Wednesday, Opening_Hours_Thursday, "
        + "Opening_Hours_Friday, Opening_Hours_Saturday, "
        + "'closed', date) AS open FROM libraries ");
    
    finalDf.show();
  }

  private static Dataset<Row> createDataframe(SparkSession spark) {
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "date_str",
            DataTypes.StringType,
            false) });

    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create("2019-03-11 14:30:00"));
    rows.add(RowFactory.create("2019-04-27 16:00:00"));
    rows.add(RowFactory.create("2020-01-26 05:00:00"));

    return spark
        .createDataFrame(rows, schema)
        .withColumn("date", to_timestamp(col("date_str")))
        .drop("date_str");
  }
}
