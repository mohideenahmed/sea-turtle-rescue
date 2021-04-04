from pip._vendor.pyparsing import col
from pyspark.python.pyspark.shell import spark
from pyspark.sql.functions import *


if __name__ == "__main__":
    df = spark.read.json("C:/Users/ahmed.mohideen/Desktop/PYTHON/pythonProject/HelloSpark/data/sample.json", multiLine = "true")
    #print(df.schema.names)


    for column_name in df.schema.names:
        print(df.schema[column_name].dataType)


    #sampleDF = rawDF.withColumnRenamed("id", "key")
    # sampleDF = rawDF.select("batters.batter.id")
    #sampleDF = rawDF.withColumn("batters.batter.id1",col("batters.batter.id"))
    #sampleDf = rawDF.
    # schemaDF = rawDF.select("column4").schema.names


    # schemaDF.show(truncate=False)
    # tempDF = rawDF.select("column4.column6.column7[0]")
    # sampleDF = rawDF.schema().fields()
    # tempDF.show(truncate=False)