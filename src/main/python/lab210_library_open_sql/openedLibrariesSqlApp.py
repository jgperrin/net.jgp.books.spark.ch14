"""
 Custom UDF to check if in range.

 @author rambabu.posa
"""
import os
from datetime import datetime
from pyspark.sql import (SparkSession, functions as F)
from pyspark.sql.types import (StructType,StructField,
                               StringType,IntegerType, BooleanType)

def get_absolute_file_path(path, filename):
    current_dir = os.path.dirname(__file__)
    relative_path = "{}{}".format(path, filename)
    absolute_file_path = os.path.join(current_dir, relative_path)
    return absolute_file_path

def create_dataframe(spark):
    schema = StructType([
        StructField('id', IntegerType(), False),
        StructField('date_str', StringType(), False)
    ])
    rows = [
        (1, "2019-03-11 14:30:00"),
        (2, "2019-04-27 16:00:00"),
        (3, "2020-01-26 05:00:00")
    ]
    df = spark.createDataFrame(rows, schema)
    df = df.withColumn("date", F.to_timestamp(F.col("date_str"))) \
        .drop("date_str")
    return df

def is_open(hoursMon, hoursTue, hoursWed, hoursThu, hoursFri, hoursSat, hoursSun, dateTime):
    if(dateTime.weekday() == 0):
        hours = hoursMon
    elif(dateTime.weekday() == 1):
        hours = hoursTue
    elif(dateTime.weekday() == 2):
        hours = hoursWed
    elif(dateTime.weekday() == 3):
        hours = hoursThu
    elif(dateTime.weekday() == 4):
        hours = hoursFri
    elif(dateTime.weekday() == 5):
        hours = hoursSat
    elif(dateTime.weekday() == 6):
        hours = hoursSun

    time = dateTime.time()
    print('Opening hours = {}, actual time = {} '.format(hours, time))

    if hours.lower() == 'closed':
        return False
    else:
        start_time_in_hours = datetime.strptime(hours.split("-")[0], '%H:%M').time()
        end_time_in_hours = datetime.strptime(hours.split("-")[1][0:5], '%H:%M').time()
        start_time_in_secs = start_time_in_hours.hour*3600 + start_time_in_hours.minute*60
        end_time_in_secs = end_time_in_hours.hour*3600 + end_time_in_hours.minute*60
        actual_time_in_secs = dateTime.time().hour*3600 + dateTime.time().minute*60
        if actual_time_in_secs>=start_time_in_secs and actual_time_in_secs <= end_time_in_secs:
            return True
        else:
            return False

def main(spark):
    path = '../../../../data/south_dublin_libraries/'
    filename = "sdlibraries.csv"
    absolute_file_path = get_absolute_file_path(path, filename)
    librariesDf = spark.read.format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .option("encoding", "cp1252") \
        .load(absolute_file_path) \
        .drop("Administrative_Authority", "Address1", "Address2", "Town", "Postcode",
              "County", "Phone", "Email", "Website", "Image", "WGS84_Latitude", "WGS84_Longitude")

    librariesDf.show(truncate=False)
    librariesDf.printSchema()

    dateTimeDf = create_dataframe(spark).drop("id")
    dateTimeDf.show(truncate=False)
    dateTimeDf.printSchema()

    df = librariesDf.crossJoin(dateTimeDf)
    df.createOrReplaceTempView("libraries")
    df.show(truncate=False)

    # this is how to register an UDF for DataFrame and Dataset APIs
    #is_open = F.udf('is_open', BooleanType())
    # To register UDF to use in Spark SQL API
    spark.udf.register("is_open", is_open, BooleanType())

    sqlQuery = """
    SELECT Council_ID, Name, date, 
        is_open(Opening_Hours_Monday, Opening_Hours_Tuesday,
            Opening_Hours_Wednesday, Opening_Hours_Thursday, 
            Opening_Hours_Friday, Opening_Hours_Saturday, 'closed', date) as open
    FROM libraries 
    """

    # Using SQL
    finalDf = spark.sql(sqlQuery)

    finalDf.show()
    finalDf.printSchema()


if __name__ == '__main__':
    # Creates a session on a local master
    spark = SparkSession.builder.appName("Custom UDF to check if in range") \
        .master("local[*]").getOrCreate()
    # Comment this line to see full log
    spark.sparkContext.setLogLevel('error')
    main(spark)
    spark.stop()