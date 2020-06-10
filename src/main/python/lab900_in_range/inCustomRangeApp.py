"""
 Custom UDF to check if in range.

 @author rambabu.posa
"""
from datetime import datetime
from pyspark.sql import (SparkSession, functions as F)
from pyspark.sql.types import (StructType,StructField,
                               StringType, BooleanType)

def createDataframe(spark: SparkSession):
    schema = StructType([
        StructField('id', StringType(), False),
        StructField('time', StringType(), False),
        StructField('range', StringType(), False)
    ])

    rows = [
        ("id1", "2019-03-11 05:00:00", "00h00-07h30;23h30-23h59"),
        ("id2", "2019-03-11 09:00:00", "00h00-07h30;23h30-23h59"),
        ("id3", "2019-03-11 10:30:00", "00h00-07h30;23h30-23h59")
    ]
    return spark.createDataFrame(rows, schema)

def in_range(range, event):
    range_list = range.split(';')
    for item in range_list:
        start_time_in_hours = datetime.strptime(item.split("-")[0], '%Hh%M').time()
        end_time_in_hours = datetime.strptime(item.split("-")[1][0:5], '%Hh%M').time()
        start_time_in_secs = start_time_in_hours.hour*3600 + start_time_in_hours.minute*60
        end_time_in_secs = end_time_in_hours.hour*3600 + end_time_in_hours.minute*60
        if event>=start_time_in_secs and event <= end_time_in_secs:
            return True
        else:
            return False

def main(spark):
    df = createDataframe(spark)
    df.show(truncate=False)

    df = df.withColumn("date", F.date_format(F.col("time"), "yyyy-MM-dd HH:mm:ss.SSSS")) \
        .withColumn("h", F.hour(F.col("date"))) \
        .withColumn("m", F.minute(F.col("date"))) \
        .withColumn("s", F.second(F.col("date"))) \
        .withColumn("event", F.expr("h*3600 + m*60 +s")) \
        .drop("date","h","m","s")

    df.show(truncate=False)

    inRange = F.udf(in_range, BooleanType())

    df = df.withColumn("between", inRange(F.col("range"), F.col("event")))

    df.show(truncate=False)


if __name__ == '__main__':
    # Creates a session on a local master
    spark = SparkSession.builder.appName("Custom UDF to check if in range") \
        .master("local[*]").getOrCreate()
    # Comment this line to see full log
    spark.sparkContext.setLogLevel('error')
    main(spark)
    spark.stop()