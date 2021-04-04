import sys

from pyspark import SparkConf
from pyspark.sql import *

from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survey_df, count_by_country

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder\
        .config(conf=conf)\
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage : HelloSparkSQL <filename>")
        sys.exit(-1)

    surveyDF = spark.read\
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(sys.argv[1])

    surveyDF.createOrReplaceTempView("survey_tbl")
    countDF = spark.sql("select Country, count(1) as count from survey_tbl where Age<40 group by Country")

    surveyDF.show()
    countDF.show()