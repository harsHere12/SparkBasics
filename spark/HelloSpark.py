from pyspark.sql import SparkSession
from lib.utils import get_spark_app_config



if __name__ == "__main__" :
    print("Hello Spark!")
    conf = get_spark_app_config()
    spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("./datasource/Job_Placement_Data.csv")

    df.show()
    spark.stop()
    print("Spark Session Stopped ")


