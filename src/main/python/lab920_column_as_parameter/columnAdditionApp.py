"""
 Column Additions via UDF.

 @author rambabu.posa
"""
from pyspark.sql import (SparkSession, functions as F)
from pyspark.sql.types import (StructType,StructField, IntegerType)

def createDataframe(spark: SparkSession):
    schema = StructType([
        StructField('c0', IntegerType(), False),
        StructField('c1', IntegerType(), False),
        StructField('c2', IntegerType(), False),
        StructField('c3', IntegerType(), False),
        StructField('c4', IntegerType(), False),
        StructField('c5', IntegerType(), False),
        StructField('c6', IntegerType(), False),
        StructField('c7', IntegerType(), False)
    ])

    rows = [
        (1, 2, 4, 8,16, 32, 64, 128),
        (0, 0, 0, 0,0, 0, 0, 0),
        (1, 1, 1, 1,1, 1, 1, 1)
    ]
    return spark.createDataFrame(rows, schema)

def column_addition(col):
    res = 0
    for value in col:
        res = res + value

    return res

def main(spark):
    df = createDataframe(spark)
    df.show(truncate=False)

    add_col = F.udf(column_addition, IntegerType())

    cols =[df.c0, df.c1, df.c2, df.c3,
           df.c4, df.c5, df.c6, df.c7]

    col = F.array(*cols)

    df = df.withColumn("sum", add_col(col))

    df.show(truncate=False)


if __name__ == '__main__':
    # Creates a session on a local master
    spark = SparkSession.builder.appName("Column addition") \
        .master("local[*]").getOrCreate()
    # Comment this line to see full log
    spark.sparkContext.setLogLevel('error')
    main(spark)
    spark.stop()