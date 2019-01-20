from pyspark import SparkContext
from pyspark.shell import spark
from pyspark.sql import SQLContext

sc = SparkContext(appName="query19")
sqlContext = SQLContext(sc)

customer = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/customer.avro")

lineitem = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/lineitem.avro")

nation = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/nation.avro")

orders = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/orders.avro")

part = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/part.avro")

partsupp = spark.read.format("com.databricks.spark.avro") \
    .load("hdfs://namenode:8020/hossein-avro-data/partsupp.avro")

region = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/region.avro")

supplier = spark.read.format("com.databricks.spark.avro") \
    .load("hdfs://namenode:8020/hossein-avro-data/supplier.avro")


