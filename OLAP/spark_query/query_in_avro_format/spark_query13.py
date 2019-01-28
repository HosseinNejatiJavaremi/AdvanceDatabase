from pyspark import SparkContext
from pyspark.shell import spark
from pyspark.sql import SQLContext

sc = SparkContext(appName="query13")
sqlContext = SQLContext(sc)

customer = spark.read.format("com.databricks.spark.avro") \
    .load("hdfs://namenode:8020/hossein-avro-data/customer.avro")

orders = spark.read.format("com.databricks.spark.avro") \
    .load("hdfs://namenode:8020/hossein-avro-data/orders.avro")

from pyspark.sql import functions as F

query13 = customer.join(orders, (customer.C_CUSTKEY == orders.O_CUSTKEY)
                        & (orders.O_COMMENT.like('%unusual%accounts%')), "left_outer") \
    .groupBy(orders.O_CUSTKEY) \
    .agg(F.count(orders.O_CUSTKEY).alias("c_count"))

query13 = query13.groupBy("c_count") \
    .agg(F.count(orders.O_CUSTKEY).alias("custdist"))

query13 = query13.sort(query13.custdist.desc(), query13.c_count.desc())

query13.show()
