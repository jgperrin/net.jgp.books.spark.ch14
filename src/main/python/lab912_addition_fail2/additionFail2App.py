"""
 Additions via UDF.

 @author rambabu.posa
"""
from pyspark.sql import (SparkSession, functions as F)
from pyspark.sql.types import (StructType,StructField,
                               StringType, IntegerType)

def createDataframe(spark: SparkSession):
    schema = StructType([
        StructField('fname', StringType(), False),
        StructField('lname', StringType(), False),
        StructField('score1', IntegerType(), False),
        StructField('score2', IntegerType(), False)
    ])

    rows = [
        ("Jean-Georges", "Perrin", 123, 456),
        ("Jacek", "Laskowski", 147, 758),
        ("Holden", "Karau", 258, 369)
    ]
    return spark.createDataFrame(rows, schema)

def add_string(a:str, b:str) -> str:
    return a+b

def add_int(a:int, b:int) -> int:
    return a-b

def main(spark):
    df = createDataframe(spark)
    df.show(truncate=False)

    add = F.udf(add_string, StringType())
    add = F.udf(add_int, StringType())

    # Here we can observe this error
    # TypeError: unsupported operand type(s) for -: 'str' and 'str'
    df = df.withColumn("concat",
                       add(F.col("fname"), F.col("lname")))
    df.show(truncate=False)

    df = df.withColumn("score",
                       add(F.col("score1"), F.col("score2")))
    df.show(truncate=False)

if __name__ == '__main__':
    # Creates a session on a local master
    spark = SparkSession.builder.appName("Addition") \
        .master("local[*]").getOrCreate()
    # Comment this line to see full log
    spark.sparkContext.setLogLevel('error')
    main(spark)
    spark.stop()