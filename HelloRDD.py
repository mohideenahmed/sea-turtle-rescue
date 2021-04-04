import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession
from lib.logger import Log4J


if __name__ == "__main__":
    conf = SparkConf().\
        setMaster("local[3]").\
        setAppName("HelloRDD")

    spark = SparkSession.\
        builder.\
        config(conf=conf).\
        getOrCreate()

    sc = spark.SparkContext
    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage : HelloSpark <filename>")
        sys.exit(-1)

    linesRDD = sc.textFile(sys.argv[1])
