from collections import namedtuple
from pyspark.sql import SparkSession
from lib.utils import get_spark_app_config


if __name__ == "__main__" :
    print("Hello Spark!")
    conf = get_spark_app_config()
    spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()


    sc = spark.sparkContext
    linesRDD = sc.textFile("./data/Job_Placement_Data.csv")
    cols = []
    for col in linesRDD.first().split(","):
        cols.append(col)
    partitioned_RDD = linesRDD.repartition(2)
    print("Number of Partitions: ", partitioned_RDD.getNumPartitions())
    colsRDD = partitioned_RDD.map(lambda line : line.replace('"','').split(","))
    PlacementRecord = namedtuple('PlacementRecord', cols)
    # placement_record =  get_col_tuple(cols)
    selectRDD = colsRDD.map(lambda col: PlacementRecord(*col))
    filterRDD = selectRDD.filter(lambda cols: cols[0] == "F")
    groupRDD = filterRDD.map(lambda row : (row[0],1))
    femalesRDD = groupRDD.reduceByKey(lambda x,y : x+y)
    for line in femalesRDD.collect() :
        print(line)
    # print(selectRDD.collect())
    spark.stop()
    print("Spark Session Stopped ")


