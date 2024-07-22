from pyspark.sql import SparkSession

if __name__ == "__main__" :
    print("Hello Spark!")

    spark = SparkSession.builder \
            .appName("Hello Spark") \
            .master("local[3]") \
            .getOrCreate()
    spark.stop()
    print("Spark Session Stopped ")